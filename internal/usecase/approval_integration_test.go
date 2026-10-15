//go:build integration

package usecase_test

import (
	"context"
	"path/filepath"
	"sync"
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

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"
	"github.com/cbl/cbl-kit/storagex"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

func setupApprovalDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payout_approval"),
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

// approvalPayout создает выплату в состоянии requested.
func approvalPayout(t *testing.T, repo *storage.Repository, amountMinor int64) storage.Payout {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	payout := storage.Payout{
		ID:                 id.String(),
		MerchantID:         "m-approval",
		IdempotencyKey:     "key-" + id.String(),
		RequestHash:        "hash",
		AmountMinor:        amountMinor,
		AmountAsset:        "USDT",
		AmountDecimals:     2,
		Network:            "trc20",
		DestinationAddress: "TJRabPrwbZy45sbavfcjinPJC18kjpRTv8",
	}
	require.NoError(t, repo.Create(context.Background(), payout))
	return payout
}

// approvalServiceAt собирает сервис апрува с политикой из JSON.
func approvalServiceAt(t *testing.T, repo *storage.Repository, rawPolicy string) *usecase.ApprovalService {
	t.Helper()
	policy, err := domain.ParseApprovalPolicy(rawPolicy)
	require.NoError(t, err)
	return usecase.NewApprovalService(repo, zap.NewNop(), usecase.WithApprovalPolicy(policy))
}

func TestIntegrationApprovalTwoOperators(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)

	first, err := service.Approve(ctx, payout.ID, "op-1", true, "checked")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), first.State, "первый голос не переводит в approved")

	second, err := service.Approve(ctx, payout.ID, "op-2", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), second.State)
	assert.Equal(t, int64(2), second.StateVersion)

	var votes int64
	require.NoError(t, db.Table("payout_approvals").Where("payout_id = ?", payout.ID).Count(&votes).Error)
	assert.Equal(t, int64(2), votes)

	_, history, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	last := history[len(history)-1]
	assert.Equal(t, "operator approved", last.Reason)
	assert.Equal(t, "requested", last.FromState)
}

func TestIntegrationApprovalRepeatSameOperator(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)
	result, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), result.State)

	var votes int64
	require.NoError(t, db.Table("payout_approvals").Where("payout_id = ?", payout.ID).Count(&votes).Error)
	assert.Equal(t, int64(1), votes, "повтор голоса не считается дважды")
}

func TestIntegrationApprovalReject(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)

	rejected, err := service.Approve(ctx, payout.ID, "op-2", false, "sanctions hit")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRejected), rejected.State)
	require.NotNil(t, rejected.FailureReason)
	assert.Equal(t, "sanctions hit", *rejected.FailureReason)

	// Отклоненный повторно: идемпотентный успех для голосовавшего, отказ для нового.
	_, err = service.Approve(ctx, payout.ID, "op-2", false, "sanctions hit")
	require.NoError(t, err)
	_, err = service.Approve(ctx, payout.ID, "op-3", true, "")
	require.ErrorIs(t, err, usecase.ErrPayoutDecided)
}

// TestIntegrationRejectionEmitsPayoutEvent проверяет, что вето оператора
// (10.5) эмитит payout.rejected в outbox в транзакции перехода (тип добавлен
// контрактом CF-3): событие и переход коммитятся вместе.
func TestIntegrationRejectionEmitsPayoutEvent(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db, storage.WithEventSink(usecase.NewPayoutEventSink(zap.NewNop())))
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)

	rejected, err := service.Approve(ctx, payout.ID, "op-2", false, "sanctions hit")
	require.NoError(t, err)
	require.Equal(t, string(domain.StateRejected), rejected.State)

	events := outboxRows(t, db, usecase.TopicPayoutEvents)
	require.Len(t, events, 2, "запрос и отклонение")
	assert.Equal(t, "payout.requested", events[0].Headers[outbox.HeaderEventType])
	assert.Equal(t, "payout.rejected", events[1].Headers[outbox.HeaderEventType])

	payload := &eventsv1.PayoutEvent{}
	require.NoError(t, proto.Unmarshal(events[1].Payload, payload))
	assert.Equal(t, eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REJECTED, payload.GetType())
	assert.Equal(t, payout.ID, payload.GetPayoutId())
	assert.Equal(t, "m-approval", payload.GetMerchantId())
	assert.Equal(t, "sanctions hit", payload.GetFailureReason())
	assert.Equal(t, uint64(2), payload.GetStateVersion(), "после создания и вето")
}

func TestIntegrationApprovalNotRequired(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"USDT":[{"below_minor":1000000,"approvals":0}],"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 100)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.ErrorIs(t, err, usecase.ErrApprovalNotRequired)

	var votes int64
	require.NoError(t, db.Table("payout_approvals").Where("payout_id = ?", payout.ID).Count(&votes).Error)
	assert.Zero(t, votes)

	loaded, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), loaded.State)
}

func TestIntegrationApprovalNotFound(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)

	_, err := service.Approve(context.Background(), uuid.Must(uuid.NewV7()).String(), "op-1", true, "")
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestIntegrationApprovalAfterDecided(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":1}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	approved, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)
	require.Equal(t, string(domain.StateApproved), approved.State)

	// Голосовавший повторно - идемпотентный успех; новый оператор - отказ.
	again, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), again.State)
	_, err = service.Approve(ctx, payout.ID, "op-2", true, "")
	require.ErrorIs(t, err, usecase.ErrPayoutDecided)
}

func TestIntegrationApprovalConcurrentOperators(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)

	payout := approvalPayout(t, repo, 5_000_000)
	ctx := context.Background()

	var waitGroup sync.WaitGroup
	errs := make(chan error, 2)
	for _, operator := range []string{"op-a", "op-b"} {
		waitGroup.Add(1)
		go func(operator string) {
			defer waitGroup.Done()
			_, err := service.Approve(ctx, payout.ID, operator, true, "")
			errs <- err
		}(operator)
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	loaded, history, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), loaded.State)

	var votes int64
	require.NoError(t, db.Table("payout_approvals").Where("payout_id = ?", payout.ID).Count(&votes).Error)
	assert.Equal(t, int64(2), votes)

	approvedTransitions := 0
	for _, row := range history {
		if row.ToState == string(domain.StateApproved) {
			approvedTransitions++
		}
	}
	assert.Equal(t, 1, approvedTransitions, "ровно один переход в approved при гонке голосов")
}

func TestIntegrationApprovalVoteChangeLocked(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)

	// Смена решения тем же оператором: вето не записывается, отказ.
	_, err = service.Approve(ctx, payout.ID, "op-1", false, "fraud found")
	require.ErrorIs(t, err, usecase.ErrVoteLocked)

	var stored bool
	require.NoError(t, db.Table("payout_approvals").
		Where("payout_id = ? AND operator_id = 'op-1'", payout.ID).
		Pluck("approve", &stored).Error)
	assert.True(t, stored, "зафиксированное решение неизменяемо")

	loaded, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), loaded.State)
}

func TestIntegrationApprovalAfterSagaProgress(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":1}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	approved, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)
	require.Equal(t, string(domain.StateApproved), approved.State)

	// Сага 10.2 двигает выплату дальше: повтор голоса идемпотентен,
	// новый оператор получает отказ.
	_, err = repo.Transition(ctx, payout.ID, uint64(approved.StateVersion), domain.StateReserved, storage.TransitionPatch{})
	require.NoError(t, err)

	result, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateReserved), result.State)

	_, err = service.Approve(ctx, payout.ID, "op-2", true, "")
	require.ErrorIs(t, err, usecase.ErrPayoutDecided)
}

func TestIntegrationApprovalJournalAppendOnly(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)

	res := db.Exec("UPDATE payout_approvals SET approve = false WHERE payout_id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "append-only")

	res = db.Exec("DELETE FROM payout_approvals WHERE payout_id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "append-only")

	res = db.Exec("TRUNCATE payout_approvals")
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "TRUNCATE")
}

func TestIntegrationApprovalRequiredSnapshot(t *testing.T) {
	db := setupApprovalDB(t)
	repo := storage.NewRepository(db)
	service := approvalServiceAt(t, repo, `{"default":2}`)
	ctx := context.Background()

	payout := approvalPayout(t, repo, 5_000_000)
	_, err := service.Approve(ctx, payout.ID, "op-1", true, "")
	require.NoError(t, err)

	var required int
	require.NoError(t, db.Table("payout_approvals").
		Where("payout_id = ? AND operator_id = 'op-1'", payout.ID).
		Pluck("required", &required).Error)
	assert.Equal(t, 2, required, "в голосе зафиксировано требуемое число операторов")
}
