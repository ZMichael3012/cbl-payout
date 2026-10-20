package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// Payout saga errors.
var (
	// ErrInvalidPayoutInput reports invalid CreatePayout input.
	ErrInvalidPayoutInput = errors.New("usecase: invalid payout input")
	// ErrIdempotencyConflict reports a key replay with a different request body.
	ErrIdempotencyConflict = errors.New("usecase: idempotency key reuse")
	// ErrSagaUnavailable reports that a saga step is unavailable (no Ledger/Kafka).
	ErrSagaUnavailable = errors.New("usecase: saga unavailable")
	// ErrLedgerRejected reports a business rejection by Ledger (no account,
	// insufficient funds): the reserve is impossible, the payout ends failed ->
	// compensated.
	ErrLedgerRejected = errors.New("usecase: ledger rejected the operation")
	// ErrLedgerUnavailable reports transport-level unavailability of Ledger:
	// the state does not change, the step is retried (operations are idempotent).
	ErrLedgerUnavailable = errors.New("usecase: ledger unavailable")
)

// errSagaRetry rolls back a conflicting saga attempt and starts a retry.
var errSagaRetry = errors.New("usecase: saga retry")

// Saga bounds.
const (
	// sagaAttempts sets the number of attempts on a state version conflict.
	sagaAttempts = 3
)

// CreatePayout input rules (same as the CHECK constraints of schema 10.1 and
// form 10.5). The merchant identifier matches the Ledger chart of accounts
// (2.6): the hold account is derived from it, so the formats must match.
var (
	payoutAssetPattern   = regexp.MustCompile(`^[A-Z0-9]{2,16}$`)
	payoutMerchantID     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	payoutIdempotencyKey = regexp.MustCompile(`^[A-Za-z0-9._:@-]{1,255}$`)
)

// LedgerReserveRequest describes a reserve request to Ledger (2.4).
type LedgerReserveRequest struct {
	// IdempotencyKey sets the operation idempotency key.
	IdempotencyKey string
	// AccountID sets the hold account (UUIDv5 of the chart of accounts).
	AccountID string
	// RefID sets the business reference of the hold (the payout identifier).
	RefID string
	// AmountMinor sets the hold amount in minor units.
	AmountMinor int64
	// Asset sets the asset of the amount.
	Asset string
	// Decimals sets the scale of the amount.
	Decimals int
}

// LedgerClient describes the Ledger operations needed by the saga: reserve,
// commit and release are idempotent by key; business rejections are kept
// separate from transport errors.
type LedgerClient interface {
	// Reserve creates a hold and returns its identifier.
	Reserve(ctx context.Context, req LedgerReserveRequest) (string, error)
	// CommitReserve turns the hold into a posting.
	CommitReserve(ctx context.Context, idempotencyKey, reserveID string) error
	// ReleaseReserve releases the hold (compensation).
	ReleaseReserve(ctx context.Context, idempotencyKey, reserveID, reason string) error
}

// SagaStore describes the storage operations needed by the saga: creation,
// reading, saga transaction, locking read, CAS transitions and outbox writes.
type SagaStore interface {
	// Create creates a payout in the requested state (10.1).
	Create(ctx context.Context, payout storage.Payout) error
	// Payout returns a payout by identifier.
	Payout(ctx context.Context, id string) (storage.Payout, error)
	// PayoutByMerchantKey returns a payout by idempotency key.
	PayoutByMerchantKey(ctx context.Context, merchantID, idempotencyKey string) (storage.Payout, error)
	// Transaction runs the function in a repository transaction.
	Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error
	// PayoutForUpdate returns the payout under a row lock.
	PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error)
	// TransitionTx performs a state transition in the given transaction.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error)
	// AppendOutbox appends an envelope to the outbox in the given transaction.
	AppendOutbox(ctx context.Context, tx *gorm.DB, envelope outbox.Envelope) error
}

// CreatePayoutInput describes the CreatePayout input (CF-1).
type CreatePayoutInput struct {
	// IdempotencyKey sets the creation idempotency key.
	IdempotencyKey string
	// MerchantID sets the merchant that owns the payout.
	MerchantID string
	// AmountMinor sets the amount in minor units.
	AmountMinor int64
	// Asset sets the asset of the amount.
	Asset string
	// Decimals sets the scale of the amount.
	Decimals int
	// Network sets the payout network.
	Network string
	// DestinationAddress sets the recipient address.
	DestinationAddress string
	// MerchantRef sets the merchant reference for correlation.
	MerchantRef string
	// RefundInvoiceID sets the source refund invoice: a non-empty value
	// creates a refund along the same saga path (CBL-10.6).
	RefundInvoiceID string
}

// PayoutService runs the payout saga: creation, reserve in Ledger, the
// Connector command through the outbox, and completion/compensation based on
// the gateway result.
type PayoutService struct {
	store    SagaStore
	ledger   LedgerClient
	policy   domain.ApprovalPolicy
	log      *zap.Logger
	attempts int
	clock    func() time.Time
}

// PayoutOption configures PayoutService.
type PayoutOption func(*PayoutService)

// WithPayoutClock sets the time source (completed_at of terminal states).
func WithPayoutClock(clock func() time.Time) PayoutOption {
	return func(s *PayoutService) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// NewPayoutService creates a PayoutService; ledger may be nil, in which case
// the saga steps are unavailable (ErrSagaUnavailable) while the approval flow
// 10.5 works.
func NewPayoutService(store SagaStore, ledger LedgerClient, policy domain.ApprovalPolicy, log *zap.Logger, opts ...PayoutOption) *PayoutService {
	if log == nil {
		log = zap.NewNop()
	}
	service := &PayoutService{
		store:    store,
		ledger:   ledger,
		policy:   policy,
		log:      log,
		attempts: sagaAttempts,
		clock:    time.Now,
	}
	for _, opt := range opts {
		opt(service)
	}
	return service
}

// Create creates a payout request (CF-1): idempotent by merchant key; after
// creation a small payout goes straight into the saga while a large one waits
// for approval (10.5).
func (s *PayoutService) Create(ctx context.Context, in CreatePayoutInput) (storage.Payout, error) {
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)
	in.MerchantID = strings.TrimSpace(in.MerchantID)
	in.Asset = strings.ToUpper(strings.TrimSpace(in.Asset))
	in.Network = strings.TrimSpace(in.Network)
	in.DestinationAddress = strings.TrimSpace(in.DestinationAddress)
	in.MerchantRef = strings.TrimSpace(in.MerchantRef)
	refundInvoiceID, refundErr := canonicalRefundID(in.RefundInvoiceID)
	if refundErr != nil {
		return storage.Payout{}, refundErr
	}
	in.RefundInvoiceID = refundInvoiceID
	if err := validateCreatePayout(in); err != nil {
		return storage.Payout{}, err
	}
	hash := createRequestHash(in)

	id, err := uuid.NewV7()
	if err != nil {
		return storage.Payout{}, fmt.Errorf("usecase: generate payout id: %w", err)
	}
	payout := storage.Payout{
		ID:                 id.String(),
		MerchantID:         in.MerchantID,
		IdempotencyKey:     in.IdempotencyKey,
		RequestHash:        hash,
		AmountMinor:        in.AmountMinor,
		AmountAsset:        in.Asset,
		AmountDecimals:     in.Decimals,
		Network:            in.Network,
		DestinationAddress: in.DestinationAddress,
		MerchantRef:        in.MerchantRef,
	}
	if in.RefundInvoiceID != "" {
		refund := in.RefundInvoiceID
		payout.RefundInvoiceID = &refund
	}
	createErr := s.store.Create(ctx, payout)
	switch {
	case errors.Is(createErr, storage.ErrDuplicateKey):
		return s.replay(ctx, in, hash)
	case createErr != nil:
		return storage.Payout{}, fmt.Errorf("usecase: create payout: %w", createErr)
	}
	created, err := s.store.Payout(ctx, payout.ID)
	if err != nil {
		return storage.Payout{}, err
	}
	if created.RefundInvoiceID != nil {
		s.log.Info("refund created",
			zap.String("payout_id", created.ID),
			zap.String("refund_invoice_id", *created.RefundInvoiceID))
	}
	if s.policy.RequiredApprovals(created.AmountAsset, created.AmountMinor) > 0 {
		s.log.Info("payout awaits operator approval", zap.String("payout_id", created.ID))
		return created, nil
	}
	return s.advance(ctx, created)
}

// replay returns the previous payout by idempotency key; a request body
// mismatch is ErrIdempotencyConflict. A replay with the same body resumes the
// saga: a transport failure of Reserve after the row is committed must not
// leave the payout in requested forever (Advance is idempotent).
func (s *PayoutService) replay(ctx context.Context, in CreatePayoutInput, hash string) (storage.Payout, error) {
	existing, err := s.store.PayoutByMerchantKey(ctx, in.MerchantID, in.IdempotencyKey)
	if err != nil {
		return storage.Payout{}, fmt.Errorf("usecase: load replayed payout: %w", err)
	}
	if existing.RequestHash != hash {
		return storage.Payout{}, fmt.Errorf("%w: merchant %s key %s", ErrIdempotencyConflict, in.MerchantID, in.IdempotencyKey)
	}
	state, err := domain.ParseState(existing.State)
	if err != nil {
		return storage.Payout{}, fmt.Errorf("usecase: replay payout %s: %w", existing.ID, err)
	}
	requiresApproval := s.policy.RequiredApprovals(existing.AmountAsset, existing.AmountMinor) > 0
	if !requiresApproval && (state == domain.StateRequested || state == domain.StateApproved) {
		return s.advance(ctx, existing)
	}
	return existing, nil
}

// Get returns a payout by identifier.
func (s *PayoutService) Get(ctx context.Context, payoutID string) (storage.Payout, error) {
	if _, err := uuid.Parse(payoutID); err != nil {
		return storage.Payout{}, fmt.Errorf("%w: payout_id", ErrInvalidPayoutInput)
	}
	return s.store.Payout(ctx, payoutID)
}

// Advance continues the saga for payouts in requested|approved: reserve in
// Ledger, the Connector command through the outbox. It is idempotent: a replay
// on another state returns the current payout.
func (s *PayoutService) Advance(ctx context.Context, payoutID string) (storage.Payout, error) {
	if s.ledger == nil {
		return storage.Payout{}, ErrSagaUnavailable
	}
	if _, err := uuid.Parse(payoutID); err != nil {
		return storage.Payout{}, fmt.Errorf("%w: payout_id", ErrInvalidPayoutInput)
	}
	payout, err := s.store.Payout(ctx, payoutID)
	if err != nil {
		return storage.Payout{}, err
	}
	state, err := domain.ParseState(payout.State)
	if err != nil {
		return storage.Payout{}, fmt.Errorf("usecase: advance payout %s: %w", payoutID, err)
	}
	if state != domain.StateRequested && state != domain.StateApproved {
		return payout, nil
	}
	return s.advance(ctx, payout)
}

// advance performs the "reserve + command" step: Reserve in Ledger, then a
// single DB transaction (reserved, sent_to_gateway, payout.commands), so the
// transition and the command are inseparable. A business rejection by Ledger
// ends the payout failed -> compensated without release (there is no reserve).
func (s *PayoutService) advance(ctx context.Context, payout storage.Payout) (storage.Payout, error) {
	if s.ledger == nil {
		return storage.Payout{}, ErrSagaUnavailable
	}
	accountID, err := domain.MerchantAccountID(payout.MerchantID, payout.AmountAsset, payout.AmountDecimals)
	if err != nil {
		return storage.Payout{}, fmt.Errorf("usecase: derive payout account: %w", err)
	}
	reserveID, err := s.ledger.Reserve(ctx, LedgerReserveRequest{
		IdempotencyKey: "payout:" + payout.ID + ":reserve",
		AccountID:      accountID,
		RefID:          payout.ID,
		AmountMinor:    payout.AmountMinor,
		Asset:          payout.AmountAsset,
		Decimals:       payout.AmountDecimals,
	})
	if err != nil {
		if errors.Is(err, ErrLedgerRejected) {
			// The DB and events get the canonical reason without internal
			// Ledger details (those stay in the logs only).
			s.log.Warn("payout reserve rejected by ledger",
				zap.String("payout_id", payout.ID),
				zap.Error(err))
			return s.failWithoutReserve(ctx, payout, "ledger reserve rejected")
		}
		return storage.Payout{}, fmt.Errorf("usecase: reserve payout %s: %w", payout.ID, err)
	}
	command, err := BuildPayoutCommandEnvelope(payout)
	if err != nil {
		return storage.Payout{}, err
	}
	for attempt := 0; attempt < s.attempts; attempt++ {
		var result storage.Payout
		err := s.store.Transaction(ctx, func(tx *gorm.DB) error {
			current, lockErr := s.store.PayoutForUpdate(ctx, tx, payout.ID)
			if lockErr != nil {
				return lockErr
			}
			state, parseErr := domain.ParseState(current.State)
			if parseErr != nil {
				return parseErr
			}
			if state != domain.StateRequested && state != domain.StateApproved {
				// The saga has already moved (race/replay): idempotent return.
				result = current
				return nil
			}
			reserved, transitionErr := s.store.TransitionTx(ctx, tx, current.ID, uint64(current.StateVersion), domain.StateReserved, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
				ReserveID: &reserveID,
				Reason:    "saga reserved",
			})
			if errors.Is(transitionErr, domain.ErrStateConflict) {
				return errSagaRetry
			}
			if transitionErr != nil {
				return transitionErr
			}
			sent, transitionErr := s.store.TransitionTx(ctx, tx, current.ID, uint64(reserved.StateVersion), domain.StateSentToGateway, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
				Reason: "saga command queued",
			})
			if errors.Is(transitionErr, domain.ErrStateConflict) {
				return errSagaRetry
			}
			if transitionErr != nil {
				return transitionErr
			}
			if appendErr := s.store.AppendOutbox(ctx, tx, command); appendErr != nil {
				return appendErr
			}
			result = sent
			return nil
		})
		if errors.Is(err, errSagaRetry) {
			continue
		}
		if err != nil {
			return storage.Payout{}, fmt.Errorf("usecase: advance payout %s: %w", payout.ID, err)
		}
		if result.StateVersion > payout.StateVersion {
			fields := []zap.Field{
				zap.String("payout_id", payout.ID),
				zap.Int64("state_version", result.StateVersion),
			}
			if payout.RefundInvoiceID != nil {
				fields = append(fields, zap.String("refund_invoice_id", *payout.RefundInvoiceID))
			}
			s.log.Info("payout sent to gateway", fields...)
		}
		return result, nil
	}
	return storage.Payout{}, fmt.Errorf("usecase: advance payout %s: %w", payout.ID, domain.ErrStateConflict)
}

// failWithoutReserve ends a payout that has no reserve: requested|approved ->
// failed -> compensated; release is not called (T-PAYOUT-03).
func (s *PayoutService) failWithoutReserve(ctx context.Context, payout storage.Payout, reason string) (storage.Payout, error) {
	failure := truncateReason(reason)
	for attempt := 0; attempt < s.attempts; attempt++ {
		var result storage.Payout
		err := s.store.Transaction(ctx, func(tx *gorm.DB) error {
			current, lockErr := s.store.PayoutForUpdate(ctx, tx, payout.ID)
			if lockErr != nil {
				return lockErr
			}
			state, parseErr := domain.ParseState(current.State)
			if parseErr != nil {
				return parseErr
			}
			switch state {
			case domain.StateCompensated:
				result = current
				return nil
			case domain.StateFailed:
				compensated, transitionErr := s.store.TransitionTx(ctx, tx, current.ID, uint64(current.StateVersion), domain.StateCompensated, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
					Reason: "saga compensation (no reserve)",
				})
				if errors.Is(transitionErr, domain.ErrStateConflict) {
					return errSagaRetry
				}
				if transitionErr != nil {
					return transitionErr
				}
				result = compensated
				return nil
			case domain.StateRequested, domain.StateApproved:
				failed, transitionErr := s.store.TransitionTx(ctx, tx, current.ID, uint64(current.StateVersion), domain.StateFailed, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
					FailureReason: &failure,
					Reason:        "saga reserve rejected",
				})
				if errors.Is(transitionErr, domain.ErrStateConflict) {
					return errSagaRetry
				}
				if transitionErr != nil {
					return transitionErr
				}
				compensated, transitionErr := s.store.TransitionTx(ctx, tx, current.ID, uint64(failed.StateVersion), domain.StateCompensated, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
					Reason: "saga compensation (no reserve)",
				})
				if errors.Is(transitionErr, domain.ErrStateConflict) {
					return errSagaRetry
				}
				if transitionErr != nil {
					return transitionErr
				}
				result = compensated
				return nil
			default:
				return fmt.Errorf("usecase: fail payout %s in state %s", current.ID, state)
			}
		})
		if errors.Is(err, errSagaRetry) {
			continue
		}
		if err != nil {
			return storage.Payout{}, fmt.Errorf("usecase: fail payout %s: %w", payout.ID, err)
		}
		s.log.Warn("payout failed before reserve and compensated",
			zap.String("payout_id", payout.ID),
			zap.String("reason", failure))
		return result, nil
	}
	return storage.Payout{}, fmt.Errorf("usecase: fail payout %s: %w", payout.ID, domain.ErrStateConflict)
}

// validateCreatePayout validates the CreatePayout input against the 10.1 formats.
func validateCreatePayout(in CreatePayoutInput) error {
	switch {
	case !payoutMerchantID.MatchString(in.MerchantID):
		return fmt.Errorf("%w: merchant_id", ErrInvalidPayoutInput)
	case !payoutIdempotencyKey.MatchString(in.IdempotencyKey):
		return fmt.Errorf("%w: idempotency_key", ErrInvalidPayoutInput)
	case in.AmountMinor <= 0:
		return fmt.Errorf("%w: amount", ErrInvalidPayoutInput)
	case !payoutAssetPattern.MatchString(in.Asset):
		return fmt.Errorf("%w: asset", ErrInvalidPayoutInput)
	case in.Decimals < 0 || in.Decimals > 18:
		return fmt.Errorf("%w: decimals", ErrInvalidPayoutInput)
	case in.Network == "" || len(in.Network) > 64 || hasControlRunes(in.Network):
		return fmt.Errorf("%w: network", ErrInvalidPayoutInput)
	case in.DestinationAddress == "" || len(in.DestinationAddress) > 256 || hasControlRunes(in.DestinationAddress):
		return fmt.Errorf("%w: destination_address", ErrInvalidPayoutInput)
	case len(in.MerchantRef) > 255 || hasControlRunes(in.MerchantRef):
		return fmt.Errorf("%w: merchant_ref", ErrInvalidPayoutInput)
	}
	return nil
}

// createRequestHash computes a stable hash of the creation body for idempotent
// key replay comparison. Fields are length-prefixed: joining through a
// separator would allow collisions of strings containing that separator
// internally. The refund marker enters the hash only when non-empty: hashes of
// ordinary payouts created before 10.6 must match (an idempotent replay after
// the upgrade must not give a false conflict).
func createRequestHash(in CreatePayoutInput) string {
	var builder strings.Builder
	writeLengthPrefixed := func(value string) {
		fmt.Fprintf(&builder, "%d:%s", len(value), value)
	}
	writeLengthPrefixed(in.MerchantID)
	fmt.Fprintf(&builder, "%d:%s", len(fmt.Sprint(in.AmountMinor)), fmt.Sprint(in.AmountMinor))
	writeLengthPrefixed(in.Asset)
	fmt.Fprintf(&builder, "%d:%s", len(fmt.Sprint(in.Decimals)), fmt.Sprint(in.Decimals))
	writeLengthPrefixed(in.Network)
	writeLengthPrefixed(in.DestinationAddress)
	writeLengthPrefixed(in.MerchantRef)
	if in.RefundInvoiceID != "" {
		writeLengthPrefixed(in.RefundInvoiceID)
	}
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

// canonicalRefundID validates and canonicalizes the source refund invoice:
// an empty value means an ordinary payout, a non-empty one must be a UUID
// (the nil UUID and whitespace-only strings are rejected).
func canonicalRefundID(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("%w: refund_invoice_id", ErrInvalidPayoutInput)
	}
	parsed, err := uuid.Parse(trimmed)
	if err != nil || parsed == uuid.Nil {
		return "", fmt.Errorf("%w: refund_invoice_id", ErrInvalidPayoutInput)
	}
	return parsed.String(), nil
}

// hasControlRunes reports whether a string contains control characters.
func hasControlRunes(value string) bool {
	return strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7F })
}

// truncateReason truncates the reason to the schema limit on a rune boundary:
// a split multi-byte rune would produce invalid UTF-8 and a DB rejection.
func truncateReason(reason string) string {
	if len(reason) <= maxReasonBytes {
		return reason
	}
	cut := maxReasonBytes
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
}
