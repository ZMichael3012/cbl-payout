package usecase_test

import (
	"context"
	"errors"
	"sort"
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

// fakeApprovalStore implements usecase.ApprovalStore on top of memory.
type fakeApprovalStore struct {
	payout      storage.Payout
	notFound    bool
	conflicts   int
	votes       map[string]storage.Approval
	transitions []domain.State
}

// newFakeApprovalStore creates a payout in the requested state.
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

// Transaction runs the function in a test "transaction": on error the state is
// rolled back the way the DB would do it.
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

// PayoutForUpdate returns the payout or a not-found error.
func (f *fakeApprovalStore) PayoutForUpdate(context.Context, *gorm.DB, string) (storage.Payout, error) {
	if f.notFound {
		return storage.Payout{}, storage.ErrNotFound
	}
	return f.payout, nil
}

// RecordApproval records a vote; an operator replay returns false.
func (f *fakeApprovalStore) RecordApproval(_ context.Context, _ *gorm.DB, approval storage.Approval) (bool, error) {
	if _, exists := f.votes[approval.OperatorID]; exists {
		return false, nil
	}
	f.votes[approval.OperatorID] = approval
	return true, nil
}

// ApprovalByOperator returns the operator vote.
func (f *fakeApprovalStore) ApprovalByOperator(_ context.Context, _ *gorm.DB, _, operatorID string) (storage.Approval, bool, error) {
	approval, ok := f.votes[operatorID]
	return approval, ok, nil
}

// ApprovalCount returns the number of votes.
func (f *fakeApprovalStore) ApprovalCount(context.Context, *gorm.DB, string) (int, error) {
	return len(f.votes), nil
}

// ListApprovals returns the voting operators in a fixed order (CBL-12.2).
func (f *fakeApprovalStore) ListApprovals(context.Context, string) ([]string, error) {
	operators := make([]string, 0, len(f.votes))
	for operator := range f.votes {
		operators = append(operators, operator)
	}
	sort.Strings(operators)
	return operators, nil
}

// TransitionTx performs a CAS transition on the fake payout.
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
	f.payout.StateVersion = int64(next) //nolint:gosec // test value
	if patch.FailureReason != nil {
		f.payout.FailureReason = patch.FailureReason
	}
	f.transitions = append(f.transitions, to)
	return f.payout, nil
}

// approvalPolicy builds a policy with the given auto-approval threshold.
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
	assert.Equal(t, string(domain.StateRequested), first.State, "the first vote does not complete the approval")
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
	assert.Len(t, store.votes, 1, "a repeated vote is not counted twice")
	assert.Empty(t, store.transitions)
}

func TestApprovalServiceVoteChangeRejected(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	_, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)

	// A decision change by the same operator: the veto is not recorded, rejection.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", false, "fraud found")
	require.ErrorIs(t, err, usecase.ErrVoteLocked)
	assert.True(t, store.votes["op-1"].Approve, "the recorded decision does not change")

	// The second operator completes the case: the two-step rule still applies.
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

	// The policy was lowered to one operator: a vote replay recomputes the threshold.
	relaxed := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":1}`)))
	result, err := relaxed.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), result.State, "the threshold is recomputed by the current policy")
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
	assert.Equal(t, string(domain.StateRejected), rejected.State, "a single operator veto is enough")
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
	assert.Empty(t, store.votes, "the vote is not recorded when approval is not required")
}

func TestApprovalServiceDecided(t *testing.T) {
	t.Parallel()

	store := newFakeApprovalStore(5_000_000)
	store.payout.State = string(domain.StateApproved)
	store.payout.StateVersion = 2
	store.votes["op-1"] = storage.Approval{OperatorID: "op-1", Approve: true}
	service := usecase.NewApprovalService(store, zap.NewNop(),
		usecase.WithApprovalPolicy(approvalPolicy(t, `{"default":2}`)))

	// An operator who voted before gets an idempotent success.
	result, err := service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateApproved), result.State)

	// A new operator after the decision gets a rejection.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-2", true, "")
	require.ErrorIs(t, err, usecase.ErrPayoutDecided)

	// A decided payout below the current threshold answers "already decided"
	// rather than "approval not required": state is checked before threshold.
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

	// Operator format: spaces, control characters and Cyrillic are rejected.
	for _, operator := range []string{"op 1", "op\n1", "оператор"} {
		_, err = service.Approve(context.Background(), store.payout.ID, operator, true, "")
		require.Errorf(t, err, "operator %q", operator)
		require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)
	}

	// A reason with control characters is rejected.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", true, "line\nbreak")
	require.ErrorIs(t, err, usecase.ErrInvalidApprovalInput)

	// A rejection requires a non-empty reason.
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
	assert.Equal(t, []string{store.payout.ID}, continued, "the saga continues after approved")

	// An idempotent replay of an approved payout also triggers the continuation:
	// the step could fail after the vote.
	_, err = service.Approve(context.Background(), store.payout.ID, "op-1", true, "")
	require.NoError(t, err)
	assert.Len(t, continued, 2)

	// A rejection does not start the continuation.
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
	require.NoError(t, err, "a version conflict is retried until success")
	assert.Equal(t, string(domain.StateApproved), result.State)

	// Exhausting the attempts returns a conflict.
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

// failingApprovalStore fails at the transaction level.
type failingApprovalStore struct {
	store *fakeApprovalStore
	err   error
}

// Transaction returns the configured error.
func (f failingApprovalStore) Transaction(context.Context, func(tx *gorm.DB) error) error {
	return f.err
}

// PayoutForUpdate is not called.
func (f failingApprovalStore) PayoutForUpdate(context.Context, *gorm.DB, string) (storage.Payout, error) {
	return storage.Payout{}, f.err
}

// RecordApproval is not called.
func (f failingApprovalStore) RecordApproval(context.Context, *gorm.DB, storage.Approval) (bool, error) {
	return false, f.err
}

// HasApproval is not called.
func (f failingApprovalStore) ApprovalByOperator(context.Context, *gorm.DB, string, string) (storage.Approval, bool, error) {
	return storage.Approval{}, false, f.err
}

// ApprovalCount is not called.
func (f failingApprovalStore) ApprovalCount(context.Context, *gorm.DB, string) (int, error) {
	return 0, f.err
}

// ListApprovals is not called.
func (f failingApprovalStore) ListApprovals(context.Context, string) ([]string, error) {
	return nil, f.err
}

// TransitionTx is not called.
func (f failingApprovalStore) TransitionTx(context.Context, *gorm.DB, string, uint64, domain.State, storage.TransitionPatch) (storage.Payout, error) {
	return storage.Payout{}, f.err
}
