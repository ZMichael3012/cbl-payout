package usecase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/inbox"
	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// Константы конвейера событий шлюза (CBL-003, раздел 6).
const (
	// TopicGatewayPayoutEvents задает топик результатов выплаты от Connector.
	TopicGatewayPayoutEvents = "gateway.payout.events"
	// ConsumerGroupGatewayPayouts задает группу консьюмера результатов.
	ConsumerGroupGatewayPayouts = "cbl-payout-gateway"
	// maxGatewayIdentifierBytes ограничивает идентификаторы недоверенного события.
	maxGatewayIdentifierBytes = 256
)

// errInvalidGatewayEvent сообщает о некорректном событии шлюза: такой вход
// штатно уходит в retry-топики и DLQ.
var errInvalidGatewayEvent = errors.New("usecase: invalid gateway payout event")

// errGatewayTooEarly сообщает, что событие опередило сагу: повтор через
// retry-топики (команда могла еще не уйти в шлюз).
var errGatewayTooEarly = errors.New("usecase: gateway payout event too early")

// gatewayEventKind задает распознанный тип события шлюза.
type gatewayEventKind int

const (
	// gatewayKindSubmitted задает фиксацию отправки.
	gatewayKindSubmitted gatewayEventKind = iota
	// gatewayKindConfirmed задает подтверждение шлюза.
	gatewayKindConfirmed
	// gatewayKindFailed задает неудачу шлюза.
	gatewayKindFailed
)

// gatewayEventType переводит proto-enum в доменный вид; false - неизвестное.
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

// gatewayAction задает действие консьюмера по событию и состоянию выплаты.
type gatewayAction int

const (
	// actionStale задает устаревшее событие: подтверждается без эффекта.
	actionStale gatewayAction = iota
	// actionTooEarly задает событие раньше саги: повторяется.
	actionTooEarly
	// actionSubmitted задает фиксацию данных отправки (self-переход).
	actionSubmitted
	// actionCompleted задает завершение: commit резерва и completed.
	actionCompleted
	// actionCompensate задает неудачу: failed и release (компенсация).
	actionCompensate
)

// GatewayEventStore описывает операции хранилища, нужные консьюмеру: чтение
// под row-локом и переходы в транзакции Guard (tx от kit inbox Processor).
// Row-лок сериализует вердикты по одной выплате: конкурентные confirmed и
// failed не могут одновременно дойти до Ledger до CAS.
type GatewayEventStore interface {
	// PayoutForUpdate возвращает выплату под row-локом в переданной транзакции.
	PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error)
	// TransitionTx выполняет переход состояния в переданной транзакции.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error)
}

// GatewayPayoutConsumer переводит результаты шлюза (submitted/confirmed/failed)
// в переходы саги: commit резерва перед completed, release перед compensated.
type GatewayPayoutConsumer struct {
	store    GatewayEventStore
	ledger   LedgerClient
	log      *zap.Logger
	attempts int
	clock    func() time.Time
}

// GatewayOption настраивает GatewayPayoutConsumer.
type GatewayOption func(*GatewayPayoutConsumer)

// WithGatewayClock задает источник времени (completed_at, тестовый шов).
func WithGatewayClock(clock func() time.Time) GatewayOption {
	return func(c *GatewayPayoutConsumer) {
		if clock != nil {
			c.clock = clock
		}
	}
}

// NewGatewayPayoutConsumer создает GatewayPayoutConsumer.
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

// Handle обрабатывает событие шлюза в транзакции Guard: проверяет вход,
// планирует действие по состоянию и применяет переходы с CAS-повторами.
func (c *GatewayPayoutConsumer) Handle(ctx context.Context, tx *gorm.DB, msg inbox.Message) error {
	traceparent := msg.Headers[outbox.HeaderTraceparent]
	ctx = storage.WithTraceparent(ctx, traceparent)
	var event eventsv1.GatewayPayoutEvent
	if err := proto.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("%w: unmarshal: %w", errInvalidGatewayEvent, err)
	}
	eventType, known := gatewayEventType(event.GetType())
	if !known {
		// Неизвестные значения enum (BACKWARD_TRANSITIVE может добавить типы)
		// подтверждаются без эффекта, как у consumer-а платежей (4.3).
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
		// Row-лок сериализует вердикты по выплате: внешние операции Ledger
		// не могут перемешаться с конкурентным событием.
		payout, err := c.store.PayoutForUpdate(ctx, tx, payoutID)
		if err != nil {
			// Событие не может опередить создание выплаты: повтор.
			return fmt.Errorf("usecase: gateway event: load payout %s: %w", shortGatewayID(event.GetPayoutId()), err)
		}
		if err := validateEventFee(&event, payout); err != nil {
			return err
		}
		// Привязка вердикта к команде: confirmed/failed обязаны нести
		// gateway_payout_id, совпадающий с зафиксированным submitted -
		// подделанное событие не завершает и не компенсирует выплату (DLQ).
		if eventType != gatewayKindSubmitted {
			if bindingErr := validateVerdictBinding(payout, &event); bindingErr != nil {
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
			applied, applyErr := c.applyTransition(ctx, tx, payout, domain.StateSentToGateway, submittedPatch(&event), "gateway submitted")
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
				// Подтверждение без резерва невозможно по саге: DLQ, в Ledger
				// не обращаемся.
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

// applyTransition выполняет CAS-переход; false - конфликт версии (повтор).
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

// compensate переводит выплату в failed (если еще нет) и в compensated одним
// CAS-планом; release уже выполнен вызывающим.
func (c *GatewayPayoutConsumer) compensate(ctx context.Context, tx *gorm.DB, payout storage.Payout, state domain.State, event *eventsv1.GatewayPayoutEvent) (bool, error) {
	current := payout
	if state == domain.StateSentToGateway {
		reason := strings.TrimSpace(event.GetReason())
		if reason == "" {
			reason = "gateway failed"
		}
		reason = truncateReason(reason)
		failed, err := c.store.TransitionTx(ctx, tx, current.ID, uint64(current.StateVersion), domain.StateFailed, storage.TransitionPatch{ //nolint:gosec // CHECK >= 1
			FailureReason: &reason,
			Reason:        "gateway failed",
		})
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
	c.log.Warn("payout failed at gateway and compensated",
		zap.String("payout_id", current.ID),
		zap.Int64("state_version", compensated.StateVersion))
	return true, nil
}

// planGatewayEvent планирует действие по состоянию выплаты и виду события.
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

// validateGatewayEvent проверяет недоверенное событие шлюза: идентификаторы,
// лимиты длин, управляющие символы и обязательный gateway_payout_id вердикта.
func validateGatewayEvent(event *eventsv1.GatewayPayoutEvent, eventType gatewayEventKind) error {
	if _, err := uuid.Parse(event.GetPayoutId()); err != nil {
		return fmt.Errorf("%w: payout_id", errInvalidGatewayEvent)
	}
	if eventType != gatewayKindSubmitted && strings.TrimSpace(event.GetGatewayPayoutId()) == "" {
		return fmt.Errorf("%w: gateway_payout_id is required for verdict", errInvalidGatewayEvent)
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

// validateVerdictBinding сверяет идентификатор шлюза вердикта с записанным
// submitted: подтверждение или неудача без реальной отправки не принимаются.
func validateVerdictBinding(payout storage.Payout, event *eventsv1.GatewayPayoutEvent) error {
	if payout.GatewayPayoutID == nil || *payout.GatewayPayoutID == "" {
		return fmt.Errorf("%w: verdict before submitted", errInvalidGatewayEvent)
	}
	if strings.TrimSpace(event.GetGatewayPayoutId()) != *payout.GatewayPayoutID {
		return fmt.Errorf("%w: gateway_payout_id mismatch", errInvalidGatewayEvent)
	}
	return nil
}

// validateEventFee сверяет реквизиты комиссии с ожидаемыми реквизитами выплаты.
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

// submittedPatch собирает патч данных отправки из события.
func submittedPatch(event *eventsv1.GatewayPayoutEvent) storage.TransitionPatch {
	patch := storage.TransitionPatch{Reason: "gateway submitted"}
	if gatewayID := strings.TrimSpace(event.GetGatewayPayoutId()); gatewayID != "" {
		patch.GatewayPayoutID = &gatewayID
	}
	if txid := strings.TrimSpace(event.GetTxid()); txid != "" {
		patch.TxID = &txid
	}
	return patch
}

// canonicalUUID приводит валидный UUID к канонической форме.
func canonicalUUID(raw string) string {
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return raw
	}
	return parsed.String()
}

// shortGatewayID усекает идентификатор для логов и ошибок.
func shortGatewayID(raw string) string {
	if len(raw) <= 64 {
		return raw
	}
	return raw[:64] + "..."
}
