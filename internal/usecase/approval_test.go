package usecase_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// fakeApprovalStore реализует usecase.ApprovalStore поверх памяти.
type fakeApprovalStore struct {
	payout      storage.Payout
	notFound    bool
	conflicts   int
	votes       map[string]storage.Approval
	transitions []domain.State
}

// newFakeApprovalStore создает выплату в состоянии requested.
func newFakeApprovalStore(amountMinor int64) *fakeApprovalStore {
	id, _ := uuid.NewV7()
	return &fakeApprovalStore{
		payout: storage.Payout{
			ID:             id.String(),
			MerchantID:     "m-1",
			State:          string(domain.StateRequested),
			StateVersion:   1,
			AmountMinor:    amountMinor,
			AmountAsset:    "USDT",
			AmountDecimals: 2,
		},
		votes: make(map[string]storage.Approval),
	}
}

// Transaction выполняет функцию в тестовой «транзакции»: при ошибке
// состояние откатывается, как это сделала бы БД.
func (f *fakeApprovalStore) Transaction(_ context.Context, fn func(tx *gorm.DB) error) error {
	snapshotPayout := f.payout
	snapshotVotes := make(map[string]storage.Approval, len(f.votes))
	for key, value := range f.votes {
		snapshotVotes[key] = value
	}
	snapshotTransitions := len(f.transitions)
	err := fn(nil)
	if err != nil {
		f.payout = snapshotPayout
		f.votes = snapshotVotes
		f.transitions = f.transitions[:snapshotTransitions]
	}
	return err
}

// PayoutForUpdate возвращает выплату или ошибку отсутствия.
func (f *fakeApprovalStore) PayoutForUpdate(context.Context, *gorm.DB, string) (storage.Payout, error) {
	if f.notFound {
		return storage.Payout{}, storage.ErrNotFound
	}
	return f.payout, nil
}

// RecordApproval фиксирует голос, повтор оператора - false.
func (f *fakeApprovalStore) RecordApproval(_ context.Context, _ *gorm.DB, approval storage.Approval) (bool, error) {
	if _, exists := f.votes[approval.OperatorID]; exists {
		return false, nil
	}
	f.votes[approval.OperatorID] = approval
	return true, nil
}

// ApprovalByOperator возвращает голос оператора.
func (f *fakeApprovalStore) ApprovalByOperator(_ context.Context, _ *gorm.DB, _, operatorID string) (storage.Approval, bool, error) {
	approval, ok := f.votes[operatorID]
	return approval, ok, nil
}

// ApprovalCount возвращает число голосов.
func (f *fakeApprovalStore) ApprovalCount(context.Context, *gorm.DB, string) (int, error) {
	return len(f.votes), nil
}

// TransitionTx выполняет CAS-переход над фейковой выплатой.
func (f *fakeApprovalStore) TransitionTx(_ context.Context, _ *gorm.DB, _ string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error) {
	if f.conflicts > 0 {
		f.conflicts--
		return storage.Payout{}, domain.ErrStateConflict
	}
	from, err := domain.ParseState(f.payout.State)
	if err != nil {
		return storage.Payout{}, err
	}
	next, err := domain.NextVersion(from, to, expectedVersion)
	if err != nil {
		return storage.Payout{}, err
	}
	f.payout.State = string(to)
	f.payout.StateVersion = int64(next) //nolint:gosec // тестовое значение
	if patch.FailureReason != nil {
		f.payout.FailureReason = patch.FailureReason
	}
	f.transitions = append(f.transitions, to)
	return f.payout, nil
}

// approvalPolicy собирает политику с заданным порогом автоодобрения.
func approvalPolicy(t *testing.T, raw string) domain.ApprovalPolicy {
	t.Helper()
	policy, err := domain.ParseApprovalPolicy(raw)
	require.NoError(t, err)
	return policy
}

func TestApprovalServiceTwoOperators(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	first, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "looks fine")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), first.State, "первый голос не завершает апрув")
	assert.Len(t, store.votes, 1)

	second, err := service.Approve(context.Background(), store.payout.ID, "op-2", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), second.State)
	assert.Equal(t, []domain.State{domain.StateApproved}, store.transitions)
}

func TestApprovalServiceRepeatSameOperatorIdempotent(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	result, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), result.State)
	assert.Len(t, store.votes, 1, "повтор голоса не считается дважды")
	assert.Empty(t, store.transitions)
}

func TestApprovalServiceVoteChangeRejected(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)

	// Смена решения тем же оператором: вето не записывается, отказ.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", false, "fraud found")
	require.ErrorIs(t, err, usecase.ErrVoteLocked)
	assert.True(t, store.votes["op-1"].Approve, "зафиксированное решение не меняется")

	// Дело доводит второй оператор: двухэтапность остается в силе.
	approved, err := service.Approve(context.Background(), store.payout.ID, "op-2", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), approved.State)
}

func TestApprovalServiceThresholdLoweredOnRepeat(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	strict := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))
	_, err := strict.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)

	// Политика снижена до одного оператора: повтор голоса пересчитывает порог.
	relaxed := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":1}`)))
	result, err := relaxed.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), result.State, "порог пересчитан по текущей политике")
	assert.Len(t, store.votes, 1)
}

func TestApprovalServiceRejectVeto(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	rejected, err := service.Approve(context.Background(), store.payout.ID, "op-2", false, "sanctions hit")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRejected), rejected.State, "вето одного оператора достаточно")
	require.NotNil(t, rejected.FailureReason)
	assert.Equal(t, "sanctions hit", *rejected.FailureReason)
}

func TestApprovalServiceNotRequired(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(100)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"USDT":[{"below_minor":1000000,"approvals":0}],"default":2}`)))

	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.ErrorIs(t, err, usecase.ErrApprovalNotRequired)
	assert.Empty(t, store.votes, "голос не записывается, когда апрув не нужен")
}

func TestApprovalServiceDecided(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	store.payout.State = string(domain.StateApproved)
	store.payout.StateVersion = 2
	store.votes["op-1"] = storage.Approval{OperatorID: "op-1", Approve: true}
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	// Оператор, голосовавший ранее, - идемпотентный успех.
	result, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), result.State)

	// Новый оператор после решения - отказ.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-2", true, "")
	require.ErrorIs(t, err, usecase.ErrPayoutDecided)

	// Решенная выплата ниже текущего порога отвечает «уже решена», а не
	// «апрув не требуется»: порядок проверок - состояние важнее порога.
	relaxed := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":0}`)))
	_, err = relaxed.Approve(context.Background(), store.payout.ID, "op-3", true, "")
	require.ErrorIs(t, err, usecase.ErrPayoutDecided)
}

func TestApprovalServiceNotFound(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	store.notFound = true
	service := usecase.NewApprovalService(store, zap.NewNop())
	_, err := service.Approve(context.Background(), "missing", "op-1", true, "")
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestApprovalServiceInputValidation(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	service := usecase.NewApprovalService(store, zap.NewNop())

	_, err := service.Approve(context.Background(), store.payout.ID, "  ", true, "")
	require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)

	longOperator := strings.Repeat("o", 129)
	_, err = service.Approve(context.Background(), store.payout.ID, longOperator, true, "")
	require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)

	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", true, strings.Repeat("r", 256))
	require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)

	// Формат оператора: пробелы, управляющие и кириллица отклоняются.
	for _, operator := range []string{"op 1", "op\n1", "оператор"} {
		_, err = service.Approve(context.Background(), store.payout.ID, operator, true, "")
		require.Errorf(t, err, "оператор %q", operator)
		require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)
	}

	// Причина с управляющими символами отклоняется.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", true, "line\nbreak")
	require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)

	// Отклонение требует непустой причины.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", false, "")
	require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)
}

func TestApprovalContinuationRunsOnApproved(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	continued := make([]string, 0, 1)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":1}`)),
		usecase.WithApprovalContinuation(func(_ context.Context, payoutID string) {
			continued = append(continued, payoutID)
		}))

	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, []string{store.payout.ID}, continued, "сага продолжается после approved")

	// Идемпотентный повтор одобренной выплаты тоже дергает continuation:
	// шаг мог упасть после голоса.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Len(t, continued, 2)

	// Отклонение continuation не запускает.
	other := newFakeApprovalStore(5_000_000)
	rejected := make([]string, 0, 1)
	rejectService := usecase.NewApprovalService(other, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":1}`)),
		usecase.WithApprovalContinuation(func(_ context.Context, payoutID string) {
			rejected = append(rejected, payoutID)
		}))
	_, err = rejectService.Approve(context.Background(), other.payout.ID, "op-1", false, "sanctions")
	require.NoError(t, err)
	assert.Empty(t, rejected)
}

func TestApprovalServiceConflictRetry(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	store.conflicts = 1
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":1}`)))

	result, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err, "конфликт версии повторяется до успеха")
	assert.Equal(t, string(domain.StateApproved), result.State)

	// Исчерпание попыток возвращает конфликт.
	exhausted := newFakeApprovalStore(5_000_000)
	exhausted.conflicts = 3
	exhaustedService := usecase.NewApprovalService(exhausted, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":1}`)))
	_, err = exhaustedService.Approve(context.Background(), exhausted.payout.ID, "op-1", true, "")
	require.ErrorIs(t, err, domain.ErrStateConflict)
}

func TestApprovalServiceTransactionError(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	boom := errors.New("db down")
	service := usecase.NewApprovalService(failingApprovalStore{store: store, err: boom}, zap.NewNop())
	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.ErrorIs(t, err, boom)
}

// failingApprovalStore отказывает на уровне транзакции.
type failingApprovalStore struct {
	store *fakeApprovalStore
	err   error
}

// Transaction возвращает заданную ошибку.
func (f failingApprovalStore) Transaction(context.Context, func(tx *gorm.DB) error) error {
	return f.err
}

// PayoutForUpdate не вызывается.
func (f failingApprovalStore) PayoutForUpdate(context.Context, *gorm.DB, string) (storage.Payout, error) {
	return storage.Payout{}, f.err
}

// RecordApproval не вызывается.
func (f failingApprovalStore) RecordApproval(context.Context, *gorm.DB, storage.Approval) (bool, error) {
	return false, f.err
}

// HasApproval не вызывается.
func (f failingApprovalStore) ApprovalByOperator(context.Context, *gorm.DB, string, string) (storage.Approval, bool, error) {
	return storage.Approval{}, false, f.err
}

// ApprovalCount не вызывается.
func (f failingApprovalStore) ApprovalCount(context.Context, *gorm.DB, string) (int, error) {
	return 0, f.err
}

// TransitionTx не вызывается.
func (f failingApprovalStore) TransitionTx(context.Context, *gorm.DB, string, uint64, domain.State, storage.TransitionPatch) (storage.Payout, error) {
	return storage.Payout{}, f.err
}
