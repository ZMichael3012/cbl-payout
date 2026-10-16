package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// Payout approval service errors.
var (
	// ErrInvalidApprovalInput reports an invalid operator or reason.
	ErrInvalidApprovalInput = errors.New("usecase: invalid approval input")
	// ErrPayoutDecided reports that the payout is already decided or has moved
	// into the saga while the operator has not voted before.
	ErrPayoutDecided = errors.New("usecase: payout already decided")
	// ErrApprovalNotRequired reports that the amount is below the threshold
	// and no approval is needed.
	ErrApprovalNotRequired = errors.New("usecase: approval not required")
	// ErrVoteLocked reports that the operator has already voted and the
	// decision cannot be changed.
	ErrVoteLocked = errors.New("usecase: operator vote is immutable")
)

// Approval service bounds.
const (
	// maxOperatorBytes bounds the operator identifier.
	maxOperatorBytes = 128
	// maxReasonBytes bounds the decision reason.
	maxReasonBytes = 255
	// approvalAttempts sets the number of attempts on a state version conflict.
	approvalAttempts = 3
	// sagaContinuationTimeout bounds the best-effort saga step after approval.
	sagaContinuationTimeout = 5 * time.Second
)

// ApprovalStore describes the operations needed by the approval service: the
// vote transaction, the payout locking read, the vote journal and the CAS
// transition (10.1).
type ApprovalStore interface {
	// Transaction runs the function in a repository transaction.
	Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error
	// PayoutForUpdate returns the payout under a row lock.
	PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error)
	// RecordApproval appends a vote; a replay by the same operator returns false.
	RecordApproval(ctx context.Context, tx *gorm.DB, approval storage.Approval) (bool, error)
	// ApprovalByOperator returns the operator vote (if any).
	ApprovalByOperator(ctx context.Context, tx *gorm.DB, payoutID, operatorID string) (storage.Approval, bool, error)
	// ApprovalCount returns the number of votes for the payout.
	ApprovalCount(ctx context.Context, tx *gorm.DB, payoutID string) (int, error)
	// ListApprovals returns the operators that voted, in vote order.
	ListApprovals(ctx context.Context, payoutID string) ([]string, error)
	// TransitionTx performs a state transition in the given transaction.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error)
}

// ApprovalService approves or rejects a payout by an operator vote: the
// two-step rule is the policy-required number of distinct operators (10.5).
type ApprovalService struct {
	store        ApprovalStore
	policy       domain.ApprovalPolicy
	log          *zap.Logger
	attempts     int
	continuation func(ctx context.Context, payoutID string)
}

// ApprovalOption configures ApprovalService.
type ApprovalOption func(*ApprovalService)

// Approvals returns the operators that voted on the payout, in vote order
// (the administrative read contour of CBL-12.2).
func (s *ApprovalService) Approvals(ctx context.Context, payoutID string) ([]string, error) {
	operators, err := s.store.ListApprovals(ctx, payoutID)
	if err != nil {
		return nil, fmt.Errorf("approval service: list approvals: %w", err)
	}
	return operators, nil
}

// WithApprovalPolicy sets the approval threshold policy.
func WithApprovalPolicy(policy domain.ApprovalPolicy) ApprovalOption {
	return func(s *ApprovalService) { s.policy = policy }
}

// WithApprovalContinuation sets the saga continuation after approved (10.2):
// best-effort; errors are logged by the caller and are not visible to the client.
func WithApprovalContinuation(continuation func(ctx context.Context, payoutID string)) ApprovalOption {
	return func(s *ApprovalService) { s.continuation = continuation }
}

// NewApprovalService creates an ApprovalService with the default policy
// (two operators without configuration).
func NewApprovalService(store ApprovalStore, log *zap.Logger, opts ...ApprovalOption) *ApprovalService {
	if log == nil {
		log = zap.NewNop()
	}
	service := &ApprovalService{
		store:    store,
		policy:   domain.DefaultApprovalPolicy(),
		log:      log,
		attempts: approvalAttempts,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

// errApprovalRetry rolls back the transaction of a conflicting attempt (the
// vote is not committed) and starts a retry: otherwise the vote would be
// committed without the transition.
var errApprovalRetry = errors.New("usecase: approval retry")

// Approve records an operator vote: once the policy-required number of
// distinct operators is reached, the payout transitions requested -> approved;
// with approve=false, requested -> rejected (a single operator veto). The
// operator decision is locked forever (append-only): a replay with the same
// decision is idempotent, a decision change is rejected with ErrVoteLocked.
func (s *ApprovalService) Approve(ctx context.Context, payoutID, operatorID string, approve bool, reason string) (storage.Payout, error) {
	operatorID = strings.TrimSpace(operatorID)
	reason = strings.TrimSpace(reason)
	if err := validateApprovalInput(operatorID, reason, approve); err != nil {
		return storage.Payout{}, err
	}
	for attempt := 0; attempt < s.attempts; attempt++ {
		result, err := s.approveOnce(ctx, payoutID, operatorID, approve, reason)
		if errors.Is(err, errApprovalRetry) {
			continue
		}
		if err != nil {
			return storage.Payout{}, err
		}
		return result, nil
	}
	return storage.Payout{}, fmt.Errorf("usecase: approve payout %s: %w", payoutID, domain.ErrStateConflict)
}

// validateApprovalInput validates the operator and the reason: the operator
// consists of Latin letters, digits and separators up to 128 characters (the
// identity contract 12.1); the reason is up to 255 characters without control
// characters; a rejection requires a reason.
func validateApprovalInput(operatorID, reason string, approve bool) error {
	if operatorID == "" || len(operatorID) > maxOperatorBytes || !validOperatorID(operatorID) {
		return fmt.Errorf("%w: invalid operator_id", ErrInvalidApprovalInput)
	}
	if len(reason) > maxReasonBytes || strings.ContainsFunc(reason, isControlRune) {
		return fmt.Errorf("%w: invalid reason", ErrInvalidApprovalInput)
	}
	if !approve && reason == "" {
		return fmt.Errorf("%w: rejection reason is required", ErrInvalidApprovalInput)
	}
	return nil
}

// validOperatorID validates the operator identifier format.
func validOperatorID(operatorID string) bool {
	for _, r := range operatorID {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '.', r == '_', r == ':', r == '@', r == '-':
		default:
			return false
		}
	}
	return true
}

// isControlRune reports that a rune is a control or non-printable character.
func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7F
}

// approveOnce performs one voting cycle in a transaction: locking read, vote
// write, count and a possible transition; a version conflict rolls back the
// whole attempt (errApprovalRetry), so the vote is not committed without the
// transition.
func (s *ApprovalService) approveOnce(ctx context.Context, payoutID, operatorID string, approve bool, reason string) (storage.Payout, error) {
	var (
		result       storage.Payout
		inserted     bool
		transited    domain.State
		requiredSeen int
	)
	err := s.store.Transaction(ctx, func(tx *gorm.DB) error {
		payout, err := s.store.PayoutForUpdate(ctx, tx, payoutID)
		if err != nil {
			return err
		}
		state, err := domain.ParseState(payout.State)
		if err != nil {
			return fmt.Errorf("usecase: approve payout %s: %w", payoutID, err)
		}
		required := s.policy.RequiredApprovals(payout.AmountAsset, payout.AmountMinor)
		requiredSeen = required

		// The operator decision is immutable: a replay is compared against the stored vote.
		vote, hasVote, voteErr := s.store.ApprovalByOperator(ctx, tx, payoutID, operatorID)
		if voteErr != nil {
			return voteErr
		}
		if hasVote && vote.Approve != approve {
			s.log.Warn("payout approval vote change rejected",
				zap.String("payout_id", payoutID),
				zap.String("operator_id", operatorID),
				zap.Bool("stored", vote.Approve),
				zap.Bool("requested", approve))
			return fmt.Errorf("%w: operator %s already voted", ErrVoteLocked, operatorID)
		}

		if state != domain.StateRequested {
			if hasVote {
				// A replay by an operator who has already voted: idempotent success.
				result = payout
				return nil
			}
			return ErrPayoutDecided
		}
		if required == 0 {
			return ErrApprovalNotRequired
		}
		if hasVote {
			// Replay with the same decision: the state does not change, but the
			// threshold is recomputed by the current policy (it may be lower).
			inserted = false
		} else {
			var recordErr error
			inserted, recordErr = s.store.RecordApproval(ctx, tx, storage.Approval{
				PayoutID:   payoutID,
				OperatorID: operatorID,
				Approve:    approve,
				Reason:     reason,
				Required:   required,
			})
			if recordErr != nil {
				return recordErr
			}
			if !inserted {
				// Race of two calls by the same operator: the vote was
				// committed concurrently, the recount below decides the transition.
				vote, hasVote, voteErr = s.store.ApprovalByOperator(ctx, tx, payoutID, operatorID)
				if voteErr != nil {
					return voteErr
				}
				if hasVote && vote.Approve != approve {
					return fmt.Errorf("%w: operator %s already voted", ErrVoteLocked, operatorID)
				}
			}
		}
		count, countErr := s.store.ApprovalCount(ctx, tx, payoutID)
		if countErr != nil {
			return countErr
		}
		target := domain.StateRequested
		switch {
		case !approve:
			target = domain.StateRejected
		case approve && count >= required:
			target = domain.StateApproved
		}
		if target == domain.StateRequested {
			result = payout
			return nil
		}
		patch := storage.TransitionPatch{Reason: "operator " + string(target)}
		if target == domain.StateRejected {
			failure := reason
			patch.FailureReason = &failure
		}
		updated, transitionErr := s.store.TransitionTx(ctx, tx, payoutID, uint64(payout.StateVersion), target, patch) //nolint:gosec // state_version is bounded by CHECK (>= 1)
		if errors.Is(transitionErr, domain.ErrStateConflict) {
			return errApprovalRetry
		}
		if transitionErr != nil {
			return transitionErr
		}
		transited = target
		result = updated
		return nil
	})
	if err != nil {
		return storage.Payout{}, err
	}
	s.log.Debug("payout approval processed",
		zap.String("payout_id", payoutID),
		zap.String("operator_id", operatorID),
		zap.Bool("approve", approve),
		zap.Int("required", requiredSeen),
		zap.Bool("recorded", inserted))
	if transited != "" {
		s.log.Info("payout approval decided",
			zap.String("payout_id", payoutID),
			zap.String("state", string(transited)),
			zap.Int64("state_version", result.StateVersion))
	}
	// Saga continuation: both after the transition to approved and on an
	// idempotent replay of an already approved payout (the step could fail
	// after the vote). Cancellation of the client context does not kill the
	// external step: it has its own timeout.
	if result.State == string(domain.StateApproved) && s.continuation != nil {
		continuationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sagaContinuationTimeout)
		defer cancel()
		s.continuation(continuationCtx, payoutID)
	}
	return result, nil
}
