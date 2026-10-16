//go:build e2e

// Package e2e contains the end-to-end payout saga scenario: CreatePayout puts the
// payout.commands command through the outbox and relay; the external "connector"
// reads the command and publishes gateway.payout.events; payout completes the saga
// (commit + completed) or compensates (release + compensated). Stand: real Kafka
// and PostgreSQL; Ledger is replaced by a fake (provider - KI-01).
package e2e

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/inbox/kafkaconsumer"
	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/outbox/kafkapub"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// recorderLedger records Ledger operations for the saga.
type recorderLedger struct {
	mu       sync.Mutex
	commits  []string
	releases []string
}

// Reserve returns a deterministic hold identifier.
func (l *recorderLedger) Reserve(_ context.Context, req usecase.LedgerReserveRequest) (string, error) {
	return "reserve-" + req.RefID, nil
}

// CommitReserve records a commit.
func (l *recorderLedger) CommitReserve(_ context.Context, _, reserveID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commits = append(l.commits, reserveID)
	return nil
}

// ReleaseReserve records a release.
func (l *recorderLedger) ReleaseReserve(_ context.Context, _, reserveID, _ string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releases = append(l.releases, reserveID)
	return nil
}

func setupSagaE2EPostgres(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payout_saga_e2e"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

func setupSagaE2EKafka(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	container, err := kafka.Run(ctx,
		"confluentinc/cp-kafka:7.6.1",
		kafka.WithClusterID("cbl-payout-saga-e2e"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)

	adminClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer adminClient.Close()
	admin := kadm.NewClient(adminClient)
	gateway := usecase.TopicGatewayPayoutEvents
	topics := []string{
		gateway, gateway + inbox.RetrySuffix5s, gateway + inbox.RetrySuffix1m,
		gateway + inbox.RetrySuffix10m, gateway + inbox.DLQSuffix,
		usecase.TopicPayoutCommands, usecase.TopicPayoutEvents,
	}
	for _, name := range topics {
		_, err := admin.CreateTopic(ctx, 1, 1, nil, name)
		require.NoError(t, err, name)
	}
	return brokers
}

// waitForState waits for the payout state or fails on context timeout.
func waitForState(t *testing.T, ctx context.Context, repo *storage.Repository, id string, want domain.State) storage.Payout {
	t.Helper()
	for {
		payout, err := repo.Payout(ctx, id)
		require.NoError(t, err)
		if payout.State == string(want) {
			return payout
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "payout state not reached", string(want), payout.State)
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// consumeRecords reads topic records from the beginning until want records are collected.
func consumeRecords(t *testing.T, ctx context.Context, brokers []string, topic string, want int) []kgo.Record {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("e2e-"+topic+"-"+uuid.Must(uuid.NewV7()).String()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	var records []kgo.Record
	for len(records) < want {
		fetches := consumer.PollFetches(ctx)
		if fetchErr := fetches.Err(); fetchErr != nil {
			require.FailNow(t, "poll failed", fetchErr.Error(), fmt.Sprintf("got %d of %d", len(records), want))
		}
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, *record)
		})
	}
	return records
}

// produceGatewayEvent publishes a gateway event to gateway.payout.events.
func produceGatewayEvent(t *testing.T, ctx context.Context, producer *kgo.Client, event *eventsv1.GatewayPayoutEvent) {
	t.Helper()
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	eventType := "gateway.payout." + strings.ToLower(strings.TrimPrefix(event.GetType().String(), "GATEWAY_PAYOUT_EVENT_TYPE_"))
	record := &kgo.Record{
		Topic: usecase.TopicGatewayPayoutEvents,
		Key:   []byte(event.GetPayoutId()),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: outbox.HeaderEventID, Value: []byte(uuid.Must(uuid.NewV7()).String())},
			{Key: outbox.HeaderEventType, Value: []byte(eventType)},
			{Key: outbox.HeaderProducer, Value: []byte("cbl-connector")},
		},
	}
	require.NoError(t, producer.ProduceSync(ctx, record).FirstErr())
}

func TestSagaKafkaFlow(t *testing.T) {
	db := setupSagaE2EPostgres(t)
	brokers := setupSagaE2EKafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ledger := &recorderLedger{}
	repo := storage.NewRepository(db, storage.WithEventSink(usecase.NewPayoutEventSink(zap.NewNop())))
	policy, err := domain.ParseApprovalPolicy(`{"default":0}`)
	require.NoError(t, err)
	saga := usecase.NewPayoutService(repo, ledger, policy, zap.NewNop())

	publisher, err := kafkapub.New(brokers)
	require.NoError(t, err)
	defer publisher.Close()

	relay := outbox.NewRelay(db, publisher, zap.NewNop(), outbox.WithInterval(50*time.Millisecond))
	go func() { _ = relay.Run(ctx) }()

	group := "e2e-gateway-" + uuid.Must(uuid.NewV7()).String()
	consumer := usecase.NewGatewayPayoutConsumer(repo, ledger, zap.NewNop())
	processor := inbox.NewProcessor(group, db, inbox.NewGuard(), consumer, publisher,
		inbox.NewRouter(usecase.TopicGatewayPayoutEvents, 3), zap.NewNop())
	gateway := usecase.TopicGatewayPayoutEvents
	runner, err := kafkaconsumer.New(brokers, group, []string{
		gateway, gateway + inbox.RetrySuffix5s, gateway + inbox.RetrySuffix1m, gateway + inbox.RetrySuffix10m,
	}, processor, zap.NewNop())
	require.NoError(t, err)
	defer runner.Close()
	go func() { _ = runner.Run(ctx) }()

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()

	// 1. Small payout: the command goes to payout.commands, the saga goes to sent_to_gateway.
	in := usecase.CreatePayoutInput{
		IdempotencyKey:     "saga-e2e-1",
		MerchantID:         "m-saga",
		AmountMinor:        500_000,
		Asset:              "USDT",
		Decimals:           2,
		Network:            "trc20",
		DestinationAddress: "TJRabPrwbZy45sbavfcjinPJC18kjpRTv8",
		MerchantRef:        "ref-saga",
	}
	payout, err := saga.Create(ctx, in)
	require.NoError(t, err)
	require.Equal(t, string(domain.StateSentToGateway), payout.State)

	commands := consumeRecords(t, ctx, brokers, usecase.TopicPayoutCommands, 1)
	var command eventsv1.PayoutCommand
	require.NoError(t, proto.Unmarshal(commands[0].Value, &command))
	assert.Equal(t, payout.ID, command.GetPayoutId())
	assert.Equal(t, payout.DestinationAddress, command.GetAddress())
	assert.Equal(t, int64(500_000), command.GetAmount().GetMinorUnits())
	assert.Equal(t, "payout:"+payout.ID+":command", command.GetIdempotencyKey())

	// 2. The gateway sends submitted (fixes gateway_payout_id), then
	// confirms: reserve commit and completed.
	produceGatewayEvent(t, ctx, producer, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-e2e",
		Txid:            "tx-e2e",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})
	waitForStateVersion(t, ctx, repo, payout.ID, 4)

	produceGatewayEvent(t, ctx, producer, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-e2e",
		Txid:            "tx-e2e",
		Fee:             &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	})
	completed := waitForState(t, ctx, repo, payout.ID, domain.StateCompleted)
	require.NotNil(t, completed.CompletedAt)
	assert.Equal(t, []string{"reserve-" + payout.ID}, ledger.commits)

	// 3. payout.events: requested and completed with versions.
	events := consumeRecords(t, ctx, brokers, usecase.TopicPayoutEvents, 2)
	byType := map[string]*eventsv1.PayoutEvent{}
	for _, record := range events {
		payload := &eventsv1.PayoutEvent{}
		require.NoError(t, proto.Unmarshal(record.Value, payload))
		byType[string(recordHeader(record, outbox.HeaderEventType))] = payload
	}
	requested := byType["payout.requested"]
	completedEvent := byType["payout.completed"]
	require.NotNil(t, requested)
	require.NotNil(t, completedEvent)
	assert.Equal(t, uint64(1), requested.GetStateVersion())
	assert.Equal(t, eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED, completedEvent.GetType())
	require.NotNil(t, completedEvent.GetOccurredAt())

	// 4. Second payout: submitted, then gateway rejection - release and compensated.
	failedInput := in
	failedInput.IdempotencyKey = "saga-e2e-2"
	failedPayout, err := saga.Create(ctx, failedInput)
	require.NoError(t, err)
	require.Equal(t, string(domain.StateSentToGateway), failedPayout.State)

	produceGatewayEvent(t, ctx, producer, &eventsv1.GatewayPayoutEvent{
		PayoutId:        failedPayout.ID,
		GatewayPayoutId: "gw-fail",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})
	waitForStateVersion(t, ctx, repo, failedPayout.ID, 4)

	produceGatewayEvent(t, ctx, producer, &eventsv1.GatewayPayoutEvent{
		PayoutId:        failedPayout.ID,
		GatewayPayoutId: "gw-fail",
		Reason:          "address rejected",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	})
	waitForState(t, ctx, repo, failedPayout.ID, domain.StateCompensated)
	assert.Contains(t, ledger.releases, "reserve-"+failedPayout.ID)

	// 5. Refund (CBL-10.6): the same saga path, events carry refund_invoice_id.
	refundInput := in
	refundInput.IdempotencyKey = "saga-e2e-refund"
	refundInput.RefundInvoiceID = uuid.Must(uuid.NewV7()).String()
	refundPayout, err := saga.Create(ctx, refundInput)
	require.NoError(t, err)
	require.NotNil(t, refundPayout.RefundInvoiceID)

	produceGatewayEvent(t, ctx, producer, &eventsv1.GatewayPayoutEvent{
		PayoutId:        refundPayout.ID,
		GatewayPayoutId: "gw-refund",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})
	waitForStateVersion(t, ctx, repo, refundPayout.ID, 4)
	produceGatewayEvent(t, ctx, producer, &eventsv1.GatewayPayoutEvent{
		PayoutId:        refundPayout.ID,
		GatewayPayoutId: "gw-refund",
		Txid:            "tx-refund",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	})
	waitForState(t, ctx, repo, refundPayout.ID, domain.StateCompleted)

	refundEvent := waitForPayoutEvent(t, ctx, brokers, refundPayout.ID, eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED)
	assert.Equal(t, *refundPayout.RefundInvoiceID, refundEvent.GetRefundInvoiceId(), "refund marker in events")
}

// waitForPayoutEvent waits for a payout event of the given type in payout.events.
func waitForPayoutEvent(t *testing.T, ctx context.Context, brokers []string, payoutID string, want eventsv1.PayoutEventType) *eventsv1.PayoutEvent {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(usecase.TopicPayoutEvents),
		kgo.ConsumerGroup("e2e-wait-"+uuid.Must(uuid.NewV7()).String()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	for {
		fetches := consumer.PollFetches(ctx)
		if fetchErr := fetches.Err(); fetchErr != nil {
			require.FailNow(t, "poll payout.events failed", fetchErr.Error(), payoutID)
		}
		var found *eventsv1.PayoutEvent
		fetches.EachRecord(func(record *kgo.Record) {
			payload := &eventsv1.PayoutEvent{}
			if unmarshalErr := proto.Unmarshal(record.Value, payload); unmarshalErr == nil &&
				payload.GetPayoutId() == payoutID && payload.GetType() == want {
				found = payload
			}
		})
		if found != nil {
			return found
		}
	}
}

// waitForStateVersion waits for the state version (marker of applied submitted).
func waitForStateVersion(t *testing.T, ctx context.Context, repo *storage.Repository, id string, wantVersion int64) {
	t.Helper()
	for {
		payout, err := repo.Payout(ctx, id)
		require.NoError(t, err)
		if payout.StateVersion >= wantVersion {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "payout version not reached", fmt.Sprint(wantVersion), fmt.Sprint(payout.StateVersion))
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// recordHeader returns the header value of a Kafka record.
func recordHeader(record kgo.Record, key string) []byte {
	for _, header := range record.Headers {
		if header.Key == key {
			return header.Value
		}
	}
	return nil
}
