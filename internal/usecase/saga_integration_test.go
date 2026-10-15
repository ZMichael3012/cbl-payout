//go:build integration

package usecase_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// setupSagaDB поднимает PostgreSQL с миграциями и репозиторий с эмиссией событий.
func setupSagaDB(t *testing.T) (*gorm.DB, *storage.Repository) {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payout_saga"),
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
	return db, storage.NewRepository(db, storage.WithEventSink(usecase.NewPayoutEventSink(zap.NewNop())))
}

// sagaServiceAt собирает сагу с политикой из JSON и фейковым Ledger.
func sagaServiceAt(t *testing.T, repo *storage.Repository, policyRaw string) (*usecase.PayoutService, *usecase.GatewayPayoutConsumer, *fakeLedger) {
	t.Helper()
	policy, err := domain.ParseApprovalPolicy(policyRaw)
	require.NoError(t, err)
	ledger := &fakeLedger{}
	service := usecase.NewPayoutService(repo, ledger, policy, zap.NewNop())
	consumer := usecase.NewGatewayPayoutConsumer(repo, ledger, zap.NewNop())
	return service, consumer, ledger
}

// outboxRows возвращает строки outbox по топику в порядке вставки.
func outboxRows(t *testing.T, db *gorm.DB, topic string) []outbox.Record {
	t.Helper()
	var records []outbox.Record
	require.NoError(t, db.Where("topic = ?", topic).Order("id").Find(&records).Error)
	return records
}

func TestIntegrationSagaHappyPath(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, consumer, ledger := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()

	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)
	require.Equal(t, string(domain.StateSentToGateway), payout.State)
	require.NotNil(t, payout.ReserveID)

	events := outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 1, "создание публикует payout.requested; reserved/sent_to_gateway типа не имеют")
	assert.Equal(t, "payout.requested", events[0].Headers[outbox.HeaderEventType])
	commands := outboxRows(t, db, usecase.TopicPayoutCommands)
	require.Len(t, commands, 1)

	var command eventsv1.PayoutCommand
	require.NoError(t, proto.Unmarshal(commands[0].Payload, &command))
	assert.Equal(t, payout.ID, command.GetPayoutId())
	assert.Equal(t, payout.DestinationAddress, command.GetAddress())
	assert.Equal(t, "payout:"+payout.ID+":command", command.GetIdempotencyKey())

	// Результат шлюза: submitted фиксирует gateway_payout_id, confirmed ->
	// commit -> completed.
	submitted := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-saga",
		Txid:            "tx-saga",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	}
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, submitted)))

	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-saga",
		Txid:            "tx-saga",
		Fee:             &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, event)))

	completed, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), completed.State)
	assert.Equal(t, []string{*payout.ReserveID}, ledger.commits)
	require.NotNil(t, completed.GatewayPayoutID)
	assert.Equal(t, "gw-saga", *completed.GatewayPayoutID)

	events = outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 2)
	assert.Equal(t, "payout.completed", events[1].Headers[outbox.HeaderEventType])
}

func TestIntegrationSagaFailedCompensation(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, consumer, ledger := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()

	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)

	submitted := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-fail",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	}
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, submitted)))

	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-fail",
		Reason:          "address rejected",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, event)))

	compensated, history, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompensated), compensated.State)
	assert.Equal(t, []string{*payout.ReserveID}, ledger.releases)
	require.Len(t, history, 6, "requested, reserved, sent_to_gateway, self(submitted), failed, compensated")

	events := outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 3)
	assert.Equal(t, []string{"payout.requested", "payout.failed", "payout.compensated"},
		[]string{events[0].Headers[outbox.HeaderEventType], events[1].Headers[outbox.HeaderEventType], events[2].Headers[outbox.HeaderEventType]})
}

func TestIntegrationSagaReserveRejected(t *testing.T) {
	db, repo := setupSagaDB(t)
	policy, err := domain.ParseApprovalPolicy(`{"default":0}`)
	require.NoError(t, err)
	ledger := &fakeLedger{reserveErr: usecase.ErrLedgerRejected}
	service := usecase.NewPayoutService(repo, ledger, policy, zap.NewNop())
	ctx := context.Background()

	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompensated), payout.State)
	require.NotNil(t, payout.FailureReason)
	assert.Empty(t, ledger.releases, "release без резерва не вызывается")

	events := outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 3, "requested, failed, compensated")
	assert.Equal(t, "payout.compensated", events[2].Headers[outbox.HeaderEventType])
}

func TestIntegrationSagaIdempotentReplay(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, _, _ := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()

	first, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)
	second, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)
	assert.Equal(t, first.ID, second.ID)

	commands := outboxRows(t, db, usecase.TopicPayoutCommands)
	assert.Len(t, commands, 1, "повтор не порождает вторую команду")

	changed := smallPayoutInput()
	changed.AmountMinor = 42
	_, err = service.Create(ctx, changed)
	require.ErrorIs(t, err, usecase.ErrIdempotencyConflict)
}

func TestIntegrationSagaLargeWaitsApproval(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, _, ledger := sagaServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), payout.State)
	assert.Empty(t, ledger.reserves)
	assert.Empty(t, outboxRows(t, db, usecase.TopicPayoutCommands))
	assert.Len(t, outboxRows(t, db, usecase.TopicPayoutEvents), 1, "только payout.requested")

	// Апрув продолжает сагу: approved -> reserved -> sent_to_gateway.
	ledger2 := &fakeLedger{}
	service2 := usecase.NewPayoutService(repo, ledger2, mustPolicy(t, `{"default":2}`), zap.NewNop())
	advanced, err := service2.Advance(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateSentToGateway), advanced.State)
}

func TestIntegrationSagaRefundFlow(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, consumer, ledger := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()

	refundInvoice := mustUUIDv7(t)
	in := smallPayoutInput()
	in.IdempotencyKey = "refund-1"
	in.RefundInvoiceID = refundInvoice
	payout, err := service.Create(ctx, in)
	require.NoError(t, err)
	require.Equal(t, string(domain.StateSentToGateway), payout.State)
	require.NotNil(t, payout.RefundInvoiceID)
	assert.Equal(t, refundInvoice, *payout.RefundInvoiceID)

	// Событие создания несет маркер возврата для Notification (refund.*).
	events := outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 1)
	var created eventsv1.PayoutEvent
	require.NoError(t, proto.Unmarshal(events[0].Payload, &created))
	assert.Equal(t, refundInvoice, created.GetRefundInvoiceId())
	assert.Equal(t, eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REQUESTED, created.GetType())

	// Сага возврата: submitted + confirmed -> commit + completed с маркером.
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-refund",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})))
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-refund",
		Txid:            "tx-refund",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	})))

	completed, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), completed.State)
	assert.Equal(t, []string{"reserve-" + payout.ID}, ledger.commits)

	events = outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 2)
	var completedEvent eventsv1.PayoutEvent
	require.NoError(t, proto.Unmarshal(events[1].Payload, &completedEvent))
	assert.Equal(t, eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED, completedEvent.GetType())
	assert.Equal(t, refundInvoice, completedEvent.GetRefundInvoiceId())

	// Маркер возврата неизменяем (второй барьер).
	res := db.Exec("UPDATE payouts SET refund_invoice_id = NULL, state_version = state_version + 1 WHERE id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "immutable")

	// CHECK-формат на прямом INSERT.
	rawID := mustUUIDv7(t)
	badRefund := db.Exec(`INSERT INTO payouts (id, merchant_id, idempotency_key, request_hash, state, amount_minor,
		amount_asset, amount_decimals, network, destination_address, refund_invoice_id)
		VALUES (?, 'm-1', 'bad-refund', 'hash', 'requested', 100, 'USDT', 2, 'trc20', 'Taddr', 'not-a-uuid')`, rawID)
	require.Error(t, badRefund.Error, "невалидный refund_invoice_id отклоняется CHECK-ом")
	assert.Contains(t, badRefund.Error.Error(), "refund_invoice_id")
}

// mustUUIDv7 возвращает строку нового UUIDv7.
func mustUUIDv7(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	return id.String()
}

// mustPolicy собирает политику или падает.
func mustPolicy(t *testing.T, raw string) domain.ApprovalPolicy {
	t.Helper()
	policy, err := domain.ParseApprovalPolicy(raw)
	require.NoError(t, err)
	return policy
}
