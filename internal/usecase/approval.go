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

// Ошибки сервиса апрува выплат.
var (
	// ErrInvalidApprovalInput сообщает о некорректном операторе или причине.
	ErrInvalidApprovalInput = errors.New("usecase: invalid approval input")
	// ErrPayoutDecided сообщает, что выплата уже решена/ушла в сагу, а оператор
	// не голосовал ранее.
	ErrPayoutDecided = errors.New("usecase: payout already decided")
	// ErrApprovalNotRequired сообщает, что сумма ниже порога и апрув не нужен.
	ErrApprovalNotRequired = errors.New("usecase: approval not required")
	// ErrVoteLocked сообщает, что оператор уже голосовал и решение изменить нельзя.
	ErrVoteLocked = errors.New("usecase: operator vote is immutable")
)

// Границы сервиса апрува.
const (
	// maxOperatorBytes ограничивает идентификатор оператора.
	maxOperatorBytes = 128
	// maxReasonBytes ограничивает причину решения.
	maxReasonBytes = 255
	// approvalAttempts задает число попыток при конфликте версии состояния.
	approvalAttempts = 3
	// sagaContinuationTimeout ограничивает best-effort шаг саги после апрува.
	sagaContinuationTimeout = 5 * time.Second
)

// ApprovalStore описывает операции, нужные сервису апрува: транзакция голоса,
// locking read выплаты, журнал голосов и CAS-переход (10.1).
type ApprovalStore interface {
	// Transaction выполняет функцию в транзакции репозитория.
	Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error
	// PayoutForUpdate возвращает выплату под row-локом.
	PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error)
	// RecordApproval добавляет голос; повтор того же оператора - false.
	RecordApproval(ctx context.Context, tx *gorm.DB, approval storage.Approval) (bool, error)
	// ApprovalByOperator возвращает голос оператора (если есть).
	ApprovalByOperator(ctx context.Context, tx *gorm.DB, payoutID, operatorID string) (storage.Approval, bool, error)
	// ApprovalCount возвращает число голосов по выплате.
	ApprovalCount(ctx context.Context, tx *gorm.DB, payoutID string) (int, error)
	// TransitionTx выполняет переход состояния в переданной транзакции.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error)
}

// ApprovalService подтверждает или отклоняет выплату голосом оператора:
// двухэтапность - требуемое политикой число различных операторов (10.5).
type ApprovalService struct {
	store        ApprovalStore
	policy       domain.ApprovalPolicy
	log          *zap.Logger
	attempts     int
	continuation func(ctx context.Context, payoutID string)
}

// ApprovalOption настраивает ApprovalService.
type ApprovalOption func(*ApprovalService)

// WithApprovalPolicy задает политику порогов апрува.
func WithApprovalPolicy(policy domain.ApprovalPolicy) ApprovalOption {
	return func(s *ApprovalService) { s.policy = policy }
}

// WithApprovalContinuation задает продолжение саги после approved (10.2):
// best-effort - ошибки логируются вызывающим, клиент их не видит.
func WithApprovalContinuation(continuation func(ctx context.Context, payoutID string)) ApprovalOption {
	return func(s *ApprovalService) { s.continuation = continuation }
}

// NewApprovalService создает ApprovalService с политикой по умолчанию
// (два оператора без настройки).
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

// errApprovalRetry откатывает транзакцию конфликтной попытки (голос не
// фиксируется) и запускает повтор: иначе голос закоммитился бы без перехода.
var errApprovalRetry = errors.New("usecase: approval retry")

// Approve записывает голос оператора: при наборе требуемого политикой числа
// различных операторов выплата переходит requested -> approved; при
// approve=false - requested -> rejected (вето одного оператора). Решение
// оператора фиксируется навсегда (append-only): повтор с тем же решением
// идемпотентен, смена решения отклоняется ErrVoteLocked.
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

// validateApprovalInput проверяет оператора и причину: оператор - латиница,
// цифры и разделители до 128 символов (контракт идентичности 12.1), причина -
// до 255 символов без управляющих символов; отклонение требует причины.
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

// validOperatorID проверяет формат идентификатора оператора.
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

// isControlRune сообщает, что руна - управляющий или непечатаемый символ.
func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7F
}

// approveOnce выполняет один цикл голосования в транзакции: locking read,
// запись голоса, подсчет и возможный переход; конфликт версии откатывает
// попытку целиком (errApprovalRetry) - голос не фиксируется без перехода.
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

		// Решение оператора неизменяемо: повтор сверяется с зафиксированным голосом.
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
				// Повтор оператора, уже голосовавшего: идемпотентный успех.
				result = payout
				return nil
			}
			return ErrPayoutDecided
		}
		if required == 0 {
			return ErrApprovalNotRequired
		}
		if hasVote {
			// Повтор с тем же решением: состояние не меняется, но порог
			// пересчитывается по текущей политике (она могла снизиться).
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
				// Гонка двух вызовов одного оператора: голос зафиксирован
				// параллельно, пересчет ниже решает переход.
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
		updated, transitionErr := s.store.TransitionTx(ctx, tx, payoutID, uint64(payout.StateVersion), target, patch) //nolint:gosec // state_version ограничен CHECK (>= 1)
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
	// Продолжение саги: и после перехода в approved, и при идемпотентном
	// повторе по уже одобренной выплате (шаг мог упасть после голоса).
	// Отмена клиентского контекста не убивает внешний шаг: свой таймаут.
	if result.State == string(domain.StateApproved) && s.continuation != nil {
		continuationCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sagaContinuationTimeout)
		defer cancel()
		s.continuation(continuationCtx, payoutID)
	}
	return result, nil
}
