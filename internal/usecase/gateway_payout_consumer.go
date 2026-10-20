package usecase

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"
	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// Gateway event pipeline constants (CBL-003, section 6).
const (
	// TopicGatewayPayoutEvents sets the topic of payout results from Connector.
	TopicGatewayPayoutEvents = "gateway.payout.events"
	// ConsumerGroupGatewayPayouts sets the consumer group of the results.
	ConsumerGroupGatewayPayouts = "cbl-payout-gateway"
	// maxGatewayIdentifierBytes bounds the identifiers of an untrusted event.
	maxGatewayIdentifierBytes = 256
)

// errInvalidGatewayEvent reports an invalid gateway event: such input goes to
// the retry topics and the DLQ by design.
var errInvalidGatewayEvent = errors.New("usecase: invalid gateway payout event")

// errGatewayTooEarly reports that the event outran the saga: retry through the
// retry topics (the command may not have reached the gateway yet).
var errGatewayTooEarly = errors.New("usecase: gateway payout event too early")

// gatewayEventKind sets the recognized gateway event kind.
type gatewayEventKind int

const (
	// gatewayKindSubmitted sets the submission recording.
	gatewayKindSubmitted gatewayEventKind = iota
	// gatewayKindConfirmed sets the gateway confirmation.
	gatewayKindConfirmed
	// gatewayKindFailed sets the gateway failure.
	gatewayKindFailed
)

// gatewayEventType maps the proto enum to the domain kind; false means unknown.
func gatewayEventType(value eventsv1.GatewayPayoutEventType) (gatewayEventKind, bool) {
	switch value {
	case eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED:
		return gatewayKindSubmitted, true
	case eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED:
		return gatewayKindConfirmed, true
	case eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED:
		return gatewayKindFailed, true
	default:
		return 0, false
	}
}

// gatewayAction sets the consumer action for the event and payout state.
type gatewayAction int

const (
	// actionStale sets a stale event: it is acknowledged with no effect.
	actionStale gatewayAction = iota
	// actionTooEarly sets an event ahead of the saga: it is retried.
	actionTooEarly
	// actionSubmitted sets the recording of the submission data (a self-transition).
	actionSubmitted
	// actionCompleted sets completion: reserve commit and completed.
	actionCompleted
	// actionCompensate sets failure: failed and release (compensation).
	actionCompensate
)

// GatewayEventStore describes the storage operations needed by the consumer:
// reads under a row lock and transitions in the Guard transaction (the tx of
// the kit inbox Processor). The row lock serializes verdicts for a single
// payout: concurrent confirmed and failed cannot both reach Ledger before CAS.
type GatewayEventStore interface {
	// PayoutForUpdate returns the payout under a row lock in the given transaction.
	PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error)
	// TransitionTx performs a state transition in the given transaction.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error)
}

// GatewayPayoutConsumer maps gateway results (submitted/confirmed/failed) to
// saga transitions: reserve commit before completed, release before compensated.
type GatewayPayoutConsumer struct {
	store    GatewayEventStore
	ledger   LedgerClient
	log      *zap.Logger
	attempts int
	clock    func() time.Time
}

// GatewayOption configures GatewayPayoutConsumer.
type GatewayOption func(*GatewayPayoutConsumer)

// WithGatewayClock sets the time source (completed_at, test seam).
func WithGatewayClock(clock func() time.Time) GatewayOption {
	return func(c *GatewayPayoutConsumer) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// NewGatewayPayoutConsumer creates a GatewayPayoutConsumer.
func NewGatewayPayoutConsumer(store GatewayEventStore, ledger LedgerClient, log *zap.Logger, opts ...GatewayOption) *GatewayPayoutConsumer {
	if log == nil {
		log = zap.NewNop()
	}
	consumer := &GatewayPayoutConsumer{
		store:    store,
		ledger:   ledger,
		log:      log,
		attempts: sagaAttempts,
		clock:    time.Now,
	}
	for _, opt := range opts {
		opt(consumer)
	}
	return consumer
}

// Handle processes a gateway event in the Guard transaction: it validates the
// input, plans the action by state and applies transitions with CAS retries.
func (c *GatewayPayoutConsumer) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	traceparent := msg.Headers[outbox.HeaderTraceparent]
	ctx = storage.WithTraceparent(ctx, traceparent)
	var event eventsv1.GatewayPayoutEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidGatewayEvent, err)
	}
	eventType, known := gatewayEventType(event.GetType())
	if !known {
		// Unknown enum values (BACKWARD_TRANSITIVE may add types) are
		// acknowledged with no effect, as in the payments consumer (4.3).
		c.log.Warn("gateway payout event without known type skipped",
			zap.String("payout_id", shortGatewayID(event.GetPayoutId())),
			zap.String("type", event.GetType().String()),
			zap.String("traceparent", shortGatewayID(traceparent)))
		return nil
	}
	if err := validateGatewayEvent(&event, eventType); err != nil {
		return err
	}
	payoutID := canonicalUUID(event.GetPayoutId())

	for attempt := 0; attempt < c.attempts; attempt++ {
		// The row lock serializes verdicts for the payout: external Ledger
		// operations cannot interleave with a concurrent event.
		payout, err := c.store.PayoutForUpdate(ctx, tx, payoutID)
		if err != nil {
			// An event cannot outrun payout creation: retry.
			return fmt.Errorf("usecase: gateway event: load payout %s: %w", shortGatewayID(event.GetPayoutId()), err)
		}
		if err := validateEventFee(&event, payout); err != nil {
			return err
		}
		// Verdict binding to the command: confirmed/failed must carry a
		// gateway_payout_id matching the recorded submitted, so a forged event
		// does not complete or compensate the payout (DLQ); the only exception
		// is the connector pre-submission rejection (publishFailed): a failed
		// verdict with no gateway_payout_id against a payout that never
		// recorded one releases the hold (CBL-17.26 increment D finding).
		if eventType != gatewayKindSubmitted {
			if bindingErr := validateVerdictBinding(payout, &event, eventType); bindingErr != nil {
				return bindingErr
			}
		}
		state, err := domain.ParseState(payout.State)
		if err != nil {
			return fmt.Errorf("usecase: gateway payout %s: %w", payout.ID, err)
		}
		action, err := planGatewayEvent(state, eventType)
		if err != nil {
			return err
		}
		switch action {
		case actionStale:
			c.log.Warn("stale gateway payout event skipped",
				zap.String("payout_id", payout.ID),
				zap.String("type", event.GetType().String()),
				zap.String("state", string(state)),
				zap.String("traceparent", shortGatewayID(traceparent)))
			return nil
		case actionTooEarly:
			return fmt.Errorf("usecase: gateway payout event too early for %s: %w", payout.ID, errGatewayTooEarly)
		case actionSubmitted:
			patch, patchErr := submittedPatch(&event)
			if patchErr != nil {
				return patchErr
			}
			applied, applyErr := c.applyTransition(ctx, tx, payout, domain.StateSentToGateway, patch, "gateway submitted")
			if applyErr != nil {
				return applyErr
			}
			if !applied {
				continue
			}
			return nil
		case actionCompleted:
			if c.ledger == nil {
				return ErrSagaUnavailable
			}
			if payout.ReserveID == nil || *payout.ReserveID == "" {
				// Confirmation without a reserve is impossible by the saga:
				// DLQ, Ledger is not called.
				return fmt.Errorf("%w: confirmed without reserve", errInvalidGatewayEvent)
			}
			if err := c.ledger.CommitReserve(ctx, "payout:"+payout.ID+":commit", *payout.ReserveID); err != nil {
				return fmt.Errorf("usecase: commit reserve of %s: %w", payout.ID, err)
			}
			completedAt := c.clock().UTC()
			patch := storage.TransitionPatch{Reason: "gateway confirmed", CompletedAt: &completedAt}
			if txid := strings.TrimSpace(event.GetTxid()); txid != "" {
				patch.TxID = &txid
			}
			// A legacy payout completed by a provider-bearing verdict records
			// the provider, so payout.events and the Reconciler see it.
			if payout.Provider == nil || *payout.Provider == "" {
				if provider, providerErr := normalizeProvider(event.GetProvider()); providerErr != nil {
					return providerErr
				} else if provider != "" {
					patch.Provider = &provider
				}
			}
			if fee := event.GetFee(); fee != nil {
				amount := fee.GetMinorUnits()
				asset := strings.ToUpper(strings.TrimSpace(fee.GetAsset()))
				decimals := int(fee.GetDecimals())
				patch.FeeMinor = &amount
				patch.FeeAsset = &asset
				patch.FeeDecimals = &decimals
			}
			applied, applyErr := c.applyTransition(ctx, tx, payout, domain.StateCompleted, patch, "gateway completed")
			if applyErr != nil {
				return applyErr
			}
			if !applied {
				continue
			}
			return nil
		case actionCompensate:
			if c.ledger == nil {
				return ErrSagaUnavailable
			}
			if payout.ReserveID != nil {
				if err := c.ledger.ReleaseReserve(ctx, "payout:"+payout.ID+":release", *payout.ReserveID, "gateway failed"); err != nil {
					return fmt.Errorf("usecase: release reserve of %s: %w", payout.ID, err)
				}
			}
			applied, applyErr := c.compensate(ctx, tx, payout, state, &event)
			if applyErr != nil {
				return applyErr
			}
			if !applied {
				continue
			}
			return nil
		}
	}
	return fmt.Errorf("usecase: gateway payout event for %s: %w", shortGatewayID(event.GetPayoutId()), domain.ErrStateConflict)
}

// applyTransition performs a CAS transition; false means a version conflict (retry).
func (c *GatewayPayoutConsumer) applyTransition(ctx context.Context, tx *gorm.DB, payout storage.Payout, to domain.State, patch storage.TransitionPatch, logMessage string) (bool, error) {
	updated, err := c.store.TransitionTx(ctx, tx, payout.ID, uint64(payout.StateVersion), to, patch) //nolint:gosec // CHECK >= 1
	if errors.Is(err, domain.ErrStateConflict) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("usecase: gateway %s -> %s: %w", payout.ID, to, err)
	}
	c.log.Info(logMessage,
		zap.String("payout_id", payout.ID),
		zap.String("state", string(to)),
		zap.Int64("state_version", updated.StateVersion))
	return true, nil
}

// compensate moves the payout to failed (if not yet) and then to compensated
// in one CAS plan; release has already been performed by the caller.
func (c *GatewayPayoutConsumer) compensate(ctx context.Context, tx *gorm.DB, payout storage.Payout, state domain.State, event *eventsv1.GatewayPayoutEvent) (bool, error) {
	current := payout
	preSubmission := payout.GatewayPayoutID == nil || strings.TrimSpace(*payout.GatewayPayoutID) == ""
	if state == domain.StateSentToGateway {
		reason := strings.TrimSpace(event.GetReason())
		if reason == "" {
			reason = "gateway failed"
		}
		reason = truncateReason(reason)
		patch := storage.TransitionPatch{
			FailureReason: &reason,
			Reason:        "gateway failed",
		}
		// A legacy payout failed by a provider-bearing verdict records the
		// provider, so payout.events and the Reconciler see it (mirrors the
		// completion backfill of T-PAYOUT-10).
		if payout.Provider == nil || *payout.Provider == "" {
			if provider, providerErr := normalizeProvider(event.GetProvider()); providerErr != nil {
				return false, providerErr
			} else if provider != "" {
				patch.Provider = &provider
			}
		}
		failed, err := c.store.TransitionTx(ctx, tx, current.ID, uint64(current.StateVersion), domain.StateFailed, patch) //nolint:gosec // CHECK >= 1
		if errors.Is(err, domain.ErrStateConflict) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("usecase: gateway fail %s: %w", current.ID, err)
		}
		current = failed
	}
	compensated, err := c.store.TransitionTx(ctx, tx, current.ID, uint64(current.StateVersion), domain.StateCompensated, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
		Reason: "saga compensation",
	})
	if errors.Is(err, domain.ErrStateConflict) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("usecase: gateway compensate %s: %w", current.ID, err)
	}
	// The pre_submission flag distinguishes the connector rejection before any
	// provider payout (the accepted no-id shape) from a real gateway failure
	// in incident triage.
	c.log.Warn("payout failed at gateway and compensated",
		zap.String("payout_id", current.ID),
		zap.Int64("state_version", compensated.StateVersion),
		zap.Bool("pre_submission", preSubmission))
	return true, nil
}

// planGatewayEvent plans the action by payout state and event kind.
func planGatewayEvent(state domain.State, eventType gatewayEventKind) (gatewayAction, error) {
	early := state == domain.StateRequested || state == domain.StateApproved || state == domain.StateReserved
	switch eventType {
	case gatewayKindSubmitted:
		switch {
		case state == domain.StateSentToGateway:
			return actionSubmitted, nil
		case early:
			return actionTooEarly, nil
		default:
			return actionStale, nil
		}
	case gatewayKindConfirmed:
		switch {
		case state == domain.StateSentToGateway:
			return actionCompleted, nil
		case early:
			return actionTooEarly, nil
		default:
			return actionStale, nil
		}
	case gatewayKindFailed:
		switch {
		case state == domain.StateSentToGateway || state == domain.StateFailed:
			return actionCompensate, nil
		case early:
			return actionTooEarly, nil
		default:
			return actionStale, nil
		}
	default:
		return actionStale, fmt.Errorf("%w: unknown type", errInvalidGatewayEvent)
	}
}

// validateGatewayEvent validates the untrusted gateway event: identifiers,
// length limits, control characters and the mandatory verdict gateway_payout_id
// of a confirmation (a failed verdict may omit it: the connector publishes the
// pre-submission rejection without one).
func validateGatewayEvent(event *eventsv1.GatewayPayoutEvent, eventType gatewayEventKind) error {
	if _, err := uuid.Parse(event.GetPayoutId()); err != nil {
		return fmt.Errorf("%w: payout_id", errInvalidGatewayEvent)
	}
	if eventType == gatewayKindConfirmed && strings.TrimSpace(event.GetGatewayPayoutId()) == "" {
		return fmt.Errorf("%w: gateway_payout_id is required for confirmation", errInvalidGatewayEvent)
	}
	if fee := event.GetFee(); fee != nil {
		if fee.GetMinorUnits() < 0 {
			return fmt.Errorf("%w: negative fee", errInvalidGatewayEvent)
		}
		if fee.GetDecimals() > 18 {
			return fmt.Errorf("%w: fee decimals", errInvalidGatewayEvent)
		}
		if !payoutAssetPattern.MatchString(strings.ToUpper(strings.TrimSpace(fee.GetAsset()))) {
			return fmt.Errorf("%w: fee asset", errInvalidGatewayEvent)
		}
	}
	for name, value := range map[string]string{
		"gateway_payout_id": event.GetGatewayPayoutId(),
		"txid":              event.GetTxid(),
		"reason":            event.GetReason(),
	} {
		if len(value) > maxGatewayIdentifierBytes {
			return fmt.Errorf("%w: %s is too long", errInvalidGatewayEvent, name)
		}
		if hasControlRunes(value) {
			return fmt.Errorf("%w: %s has control characters", errInvalidGatewayEvent, name)
		}
	}
	return nil
}

// validateVerdictBinding compares the verdict gateway identifier with the
// recorded submitted: the only accepted shape without a real submission is the
// connector pre-submission rejection (a failed verdict without a gateway id
// against a payout that never recorded one); every other mismatch is a DLQ
// error. A provider-bearing verdict must agree with the recorded provider
// (17.11); an empty provider is tolerated for legacy producers.
func validateVerdictBinding(payout storage.Payout, event *eventsv1.GatewayPayoutEvent, eventType gatewayEventKind) error {
	eventGatewayID := strings.TrimSpace(event.GetGatewayPayoutId())
	recorded := ""
	if payout.GatewayPayoutID != nil {
		recorded = strings.TrimSpace(*payout.GatewayPayoutID)
	}
	if recorded == "" {
		// A payout that never recorded a provider payout accepts exactly one
		// verdict shape: a failed event without a gateway id (the connector
		// pre-submission rejection, publishFailed); every other shape is a
		// verdict before submitted (DLQ).
		if eventType != gatewayKindFailed || eventGatewayID != "" {
			return fmt.Errorf("%w: verdict before submitted", errInvalidGatewayEvent)
		}
	} else if eventGatewayID != recorded {
		return fmt.Errorf("%w: gateway_payout_id mismatch", errInvalidGatewayEvent)
	}
	provider, providerErr := normalizeProvider(event.GetProvider())
	if providerErr != nil {
		return providerErr
	}
	if provider != "" && payout.Provider != nil && *payout.Provider != "" &&
		!strings.EqualFold(*payout.Provider, provider) {
		return fmt.Errorf("%w: provider mismatch", errInvalidGatewayEvent)
	}
	return nil
}

// validateEventFee compares the fee details with the expected payout details.
func validateEventFee(event *eventsv1.GatewayPayoutEvent, payout storage.Payout) error {
	fee := event.GetFee()
	if fee == nil {
		return nil
	}
	asset := strings.ToUpper(strings.TrimSpace(fee.GetAsset()))
	if asset != payout.AmountAsset || int(fee.GetDecimals()) != payout.AmountDecimals {
		return fmt.Errorf("%w: fee %s/%d does not match payout %s/%d",
			errInvalidGatewayEvent, asset, fee.GetDecimals(), payout.AmountAsset, payout.AmountDecimals)
	}
	return nil
}

// submittedPatch builds the submission data patch from the event; the provider
// is validated and normalized, an empty value is tolerated for legacy
// producers (17.11, KI-payout-11).
func submittedPatch(event *eventsv1.GatewayPayoutEvent) (storage.TransitionPatch, error) {
	patch := storage.TransitionPatch{Reason: "gateway submitted"}
	if gatewayID := strings.TrimSpace(event.GetGatewayPayoutId()); gatewayID != "" {
		patch.GatewayPayoutID = &gatewayID
	}
	if txid := strings.TrimSpace(event.GetTxid()); txid != "" {
		patch.TxID = &txid
	}
	provider, err := normalizeProvider(event.GetProvider())
	if err != nil {
		return storage.TransitionPatch{}, err
	}
	if provider != "" {
		patch.Provider = &provider
	}
	return patch, nil
}

// providerPattern validates the provider id of an untrusted gateway event.
var providerPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{1,31}$`)

// normalizeProvider lowercases and validates an optional provider id; an empty
// value means a legacy producer and is accepted.
func normalizeProvider(raw string) (string, error) {
	provider := strings.ToLower(strings.TrimSpace(raw))
	if provider == "" {
		return "", nil
	}
	if !providerPattern.MatchString(provider) {
		return "", fmt.Errorf("%w: provider", errInvalidGatewayEvent)
	}
	return provider, nil
}

// canonicalUUID brings a valid UUID to its canonical form.
func canonicalUUID(raw string) string {
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return raw
	}
	return parsed.String()
}

// shortGatewayID truncates an identifier for logs and errors.
func shortGatewayID(raw string) string {
	if len(raw) <= 64 {
		return raw
	}
	return raw[:64] + "..."
}
