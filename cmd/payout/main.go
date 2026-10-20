// main starts the HTTP and gRPC payout service of the CBL platform.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	payoutv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/payout/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/inbox/kafkaconsumer"
	"github.com/ZMichael3012/cbl-kit/outbox"
	"github.com/ZMichael3012/cbl-kit/outbox/kafkapub"
	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/ledgerclient"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/transport"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// main starts the service gRPC server, the administrative HTTP server and the metrics server.
func main() {
	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = log.Sync() }()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		fatal(log, "PG_DSN is required")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fatal(log, "connect postgres failed", zap.Error(err))
	}

	// The pool is capped following the ledger pattern (T-LED-04): gRPC and the background saga (10.2)
	// share connections; a typo in env must not exhaust max_connections.
	sqlDB, err := db.DB()
	if err != nil {
		fatal(log, "resolve sql db failed", zap.Error(err))
	}
	maxOpen := min(getenvInt("DB_MAX_OPEN_CONNS", 20), 100)
	maxIdle := min(getenvInt("DB_MAX_IDLE_CONNS", 10), maxOpen)
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	log.Info("postgres pool configured", zap.Int("max_open", maxOpen), zap.Int("max_idle", maxIdle))

	httpAddr := getenv("HTTP_ADDR", ":8080")
	grpcAddr := getenv("GRPC_ADDR", ":9080")
	metricsAddr := getenv("METRICS_ADDR", ":9280")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	migrationsDir := getenv("MIGRATIONS_DIR", "./migrations")
	if err := storagex.Migrate(ctx, db, migrationsDir); err != nil {
		fatal(log, "apply migrations failed", zap.String("dir", migrationsDir), zap.Error(err))
	}

	repo := storage.NewRepository(db, storage.WithEventSink(usecase.NewPayoutEventSink(log)))
	policy, policyErr := domain.ParseApprovalPolicy(os.Getenv("PAYOUT_APPROVAL_POLICY"))
	if policyErr != nil {
		fatal(log, "parse PAYOUT_APPROVAL_POLICY failed", zap.Error(policyErr))
	}
	log.Info("payout approval policy configured", zap.String("policy", policy.Describe()))

	// The Ledger client is needed by the saga (10.2): without LEDGER_ADDR the saga steps are unavailable,
	// while the approval flow 10.5 works.
	var ledgerClient usecase.LedgerClient
	if ledgerAddr := os.Getenv("LEDGER_ADDR"); ledgerAddr != "" {
		client, dialErr := ledgerclient.Dial(ledgerAddr)
		if dialErr != nil {
			fatal(log, "ledger client failed", zap.Error(dialErr))
		}
		defer func() { _ = client.Close() }()
		ledgerClient = client
		log.Info("ledger client configured", zap.String("addr", ledgerAddr))
	} else {
		log.Warn("LEDGER_ADDR is empty: payout saga steps are disabled")
	}

	saga := usecase.NewPayoutService(repo, ledgerClient, policy, log)
	approval := usecase.NewApprovalService(repo, log,
		usecase.WithApprovalPolicy(policy),
		usecase.WithApprovalContinuation(func(ctx context.Context, payoutID string) {
			if _, advanceErr := saga.Advance(ctx, payoutID); advanceErr != nil {
				log.Warn("payout saga continuation failed",
					zap.String("payout_id", payoutID),
					zap.Error(advanceErr))
			}
		}))

	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	httpSrv := &http.Server{Addr: httpAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsHandler(), ReadHeaderTimeout: 5 * time.Second}

	grpcSrv := grpc.NewServer()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, healthSrv)
	payoutv1.RegisterPayoutServiceServer(grpcSrv, transport.NewPayoutServer(approval, saga))
	reflection.Register(grpcSrv)

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		brokerList := splitCSV(brokers)
		publisher, pubErr := kafkapub.New(brokerList)
		if pubErr != nil {
			fatal(log, "kafka publisher failed", zap.Error(pubErr))
		}
		defer publisher.Close()

		relay := outbox.NewRelay(db, publisher, log)
		go func() {
			if runErr := relay.Run(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				log.Error("outbox relay stopped", zap.Error(runErr))
			}
		}()

		consumer := usecase.NewGatewayPayoutConsumer(repo, ledgerClient, log)
		if ledgerClient == nil {
			log.Warn("LEDGER_ADDR is empty: verdict events will retry until Ledger is configured")
		}
		processor := inbox.NewProcessor(usecase.ConsumerGroupGatewayPayouts, db, inbox.NewGuard(), consumer, publisher,
			inbox.NewRouter(usecase.TopicGatewayPayoutEvents, 3), log)
		topic := usecase.TopicGatewayPayoutEvents
		topics := []string{
			topic, topic + inbox.RetrySuffix5s, topic + inbox.RetrySuffix1m, topic + inbox.RetrySuffix10m,
		}
		runner, runErr := kafkaconsumer.New(brokerList, usecase.ConsumerGroupGatewayPayouts, topics, processor, log)
		if runErr != nil {
			fatal(log, "gateway payout consumer failed", zap.Error(runErr))
		}
		defer runner.Close()
		go func() {
			if consumeErr := runner.Run(ctx); consumeErr != nil && !errors.Is(consumeErr, context.Canceled) {
				log.Error("gateway payout consumer stopped", zap.Error(consumeErr))
			}
		}()
		log.Info("gateway payout consumer started", zap.String("group", usecase.ConsumerGroupGatewayPayouts))
	} else {
		log.Warn("KAFKA_BROKERS is empty: outbox relay and gateway consumer are disabled")
	}

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		fatal(log, "grpc listen failed", zap.String("addr", grpcAddr), zap.Error(err))
	}

	go func() {
		log.Info("http server started", zap.String("addr", httpAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", metricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			stop()
		}
	}()
	go func() {
		log.Info("grpc server started", zap.String("addr", grpcAddr))
		if serveErr := grpcSrv.Serve(grpcListener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	log.Info("shutdown finished")
}

// metricsHandler serves the metrics stub until task 0.7.
func metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# metrics endpoint placeholder (task 0.7)")
	})
	return mux
}

// fatal logs a fatal startup error and terminates the process with a non-zero
// exit code: the orchestrator must see the failure, not a "successful" exit.
func fatal(log *zap.Logger, message string, fields ...zap.Field) {
	log.Error(message, fields...)
	os.Exit(1)
}

// getenv returns the environment variable value or the fallback value.
func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getenvInt returns a positive integer from the environment variable
// or the fallback value.
func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// splitCSV parses a comma-separated list of values from an environment variable,
// discarding empty elements and surrounding whitespace.
func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}
