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

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// Ошибки саги выплаты.
var (
	// ErrInvalidPayoutInput сообщает о некорректных входных данных CreatePayout.
	ErrInvalidPayoutInput = errors.New("usecase: invalid payout input")
	// ErrIdempotencyConflict сообщает о повторе ключа с другим телом запроса.
	ErrIdempotencyConflict = errors.New("usecase: idempotency key reuse")
	// ErrSagaUnavailable сообщает, что шаг саги недоступен (нет Ledger/Kafka).
	ErrSagaUnavailable = errors.New("usecase: saga unavailable")
	// ErrLedgerRejected сообщает о бизнес-отказе Ledger (нет счета, недостаточно
	// средств): резерв невозможен, выплата завершается failed -> compensated.
	ErrLedgerRejected = errors.New("usecase: ledger rejected the operation")
	// ErrLedgerUnavailable сообщает о транспортной недоступности Ledger:
	// состояние не меняется, шаг повторяется (операции идемпотентны).
	ErrLedgerUnavailable = errors.New("usecase: ledger unavailable")
)

// errSagaRetry откатывает конфликтную попытку саги и запускает повтор.
var errSagaRetry = errors.New("usecase: saga retry")

// Границы саги.
const (
	// sagaAttempts задает число попыток при конфликте версии состояния.
	sagaAttempts = 3
)

// Правила входных данных CreatePayout (как CHECK-и схемы 10.1 и формы 10.5).
// Идентификатор мерчанта совпадает с планом счетов Ledger (2.6): из него
// выводится счет удержания, поэтому форматы обязаны совпадать.
var (
	payoutAssetPattern   = regexp.MustCompile(`^[A-Z0-9]{2,16}$`)
	payoutMerchantID     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	payoutIdempotencyKey = regexp.MustCompile(`^[A-Za-z0-9._:@-]{1,255}$`)
)

// LedgerReserveRequest описывает запрос резерва в Ledger (2.4).
type LedgerReserveRequest struct {
	// IdempotencyKey задает ключ идемпотентности операции.
	IdempotencyKey string
	// AccountID задает счет удержания (UUIDv5 плана счетов).
	AccountID string
	// RefID задает бизнес-ссылку удержания (идентификатор выплаты).
	RefID string
	// AmountMinor задает сумму удержания в минимальных единицах.
	AmountMinor int64
	// Asset задает актив суммы.
	Asset string
	// Decimals задает шкалу суммы.
	Decimals int
}

// LedgerClient описывает операции Ledger, нужные саге: резерв, коммит и release
// идемпотентны по ключам; бизнес-отказы отделены от транспортных ошибок.
type LedgerClient interface {
	// Reserve создает удержание и возвращает его идентификатор.
	Reserve(ctx context.Context, req LedgerReserveRequest) (string, error)
	// CommitReserve превращает удержание в проводку.
	CommitReserve(ctx context.Context, idempotencyKey, reserveID string) error
	// ReleaseReserve освобождает удержание (компенсация).
	ReleaseReserve(ctx context.Context, idempotencyKey, reserveID, reason string) error
}

// SagaStore описывает операции хранилища, нужные саге: создание, чтение,
// транзакция саги, locking read, CAS-переходы и запись outbox.
type SagaStore interface {
	// Create создает выплату в состоянии requested (10.1).
	Create(ctx context.Context, payout storage.Payout) error
	// Payout возвращает выплату по идентификатору.
	Payout(ctx context.Context, id string) (storage.Payout, error)
	// PayoutByMerchantKey возвращает выплату по ключу идемпотентности.
	PayoutByMerchantKey(ctx context.Context, merchantID, idempotencyKey string) (storage.Payout, error)
	// Transaction выполняет функцию в транзакции репозитория.
	Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error
	// PayoutForUpdate возвращает выплату под row-локом.
	PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error)
	// TransitionTx выполняет переход состояния в переданной транзакции.
	TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error)
	// AppendOutbox добавляет конверт в outbox в переданной транзакции.
	AppendOutbox(ctx context.Context, tx *gorm.DB, envelope outbox.Envelope) error
}

// CreatePayoutInput описывает вход CreatePayout (CF-1).
type CreatePayoutInput struct {
	// IdempotencyKey задает ключ идемпотентности создания.
	IdempotencyKey string
	// MerchantID задает мерчанта-владельца выплаты.
	MerchantID string
	// AmountMinor задает сумму в минимальных единицах.
	AmountMinor int64
	// Asset задает актив суммы.
	Asset string
	// Decimals задает шкалу суммы.
	Decimals int
	// Network задает сеть выплаты.
	Network string
	// DestinationAddress задает адрес получателя.
	DestinationAddress string
	// MerchantRef задает ссылку мерчанта для корреляции.
	MerchantRef string
}

// PayoutService ведет сагу выплаты: создание, резерв в Ledger, команда
// Connector через outbox и завершение/компенсацию по результату шлюза.
type PayoutService struct {
	store    SagaStore
	ledger   LedgerClient
	policy   domain.ApprovalPolicy
	log      *zap.Logger
	attempts int
	clock    func() time.Time
}

// PayoutOption настраивает PayoutService.
type PayoutOption func(*PayoutService)

// WithPayoutClock задает источник времени (completed_at терминальных состояний).
func WithPayoutClock(clock func() time.Time) PayoutOption {
	return func(s *PayoutService) {
		if clock != nil {
			s.clock = clock
		}
	}
}

// NewPayoutService создает PayoutService; ledger может быть nil - шаги саги
// недоступны (ErrSagaUnavailable), апрув-контур 10.5 работает.
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

// Create создает заявку на выплату (CF-1): идемпотентно по ключу мерчанта,
// после создания малая выплата сразу идет в сагу, крупная ждет апрува (10.5).
func (s *PayoutService) Create(ctx context.Context, in CreatePayoutInput) (storage.Payout, error) {
	in.IdempotencyKey = strings.TrimSpace(in.IdempotencyKey)
	in.MerchantID = strings.TrimSpace(in.MerchantID)
	in.Asset = strings.ToUpper(strings.TrimSpace(in.Asset))
	in.Network = strings.TrimSpace(in.Network)
	in.DestinationAddress = strings.TrimSpace(in.DestinationAddress)
	in.MerchantRef = strings.TrimSpace(in.MerchantRef)
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
	if s.policy.RequiredApprovals(created.AmountAsset, created.AmountMinor) > 0 {
		s.log.Info("payout awaits operator approval", zap.String("payout_id", created.ID))
		return created, nil
	}
	return s.advance(ctx, created)
}

// replay возвращает прежнюю выплату по ключу идемпотентности; несовпадение
// тела запроса - ErrIdempotencyConflict. Повтор с тем же телом возобновляет
// сагу: транспортный сбой Reserve после фиксации строки не должен оставлять
// выплату в requested навсегда (Advance идемпотентен).
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

// Get возвращает выплату по идентификатору.
func (s *PayoutService) Get(ctx context.Context, payoutID string) (storage.Payout, error) {
	if _, err := uuid.Parse(payoutID); err != nil {
		return storage.Payout{}, fmt.Errorf("%w: payout_id", ErrInvalidPayoutInput)
	}
	return s.store.Payout(ctx, payoutID)
}

// Advance продолжает сагу для выплат в requested|approved: резерв в Ledger,
// команда Connector через outbox. Идемпотентен: повтор на другом состоянии
// возвращает текущую выплату.
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

// advance выполняет шаг «резерв + команда»: Reserve в Ledger, затем одна
// транзакция БД (reserved, sent_to_gateway, payout.commands) - переход и
// команда неразделимы. Бизнес-отказ Ledger завершает выплату failed ->
// compensated без release (резерва нет).
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
			// В БД и события уходит каноническая причина без внутренних
			// деталей Ledger (они остаются только в логах).
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
				// Сага уже двинулась (гонка/повтор): идемпотентный возврат.
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
			s.log.Info("payout sent to gateway",
				zap.String("payout_id", payout.ID),
				zap.Int64("state_version", result.StateVersion))
		}
		return result, nil
	}
	return storage.Payout{}, fmt.Errorf("usecase: advance payout %s: %w", payout.ID, domain.ErrStateConflict)
}

// failWithoutReserve завершает выплату, у которой резерва нет: requested|
// approved -> failed -> compensated; release не вызывается (T-PAYOUT-03).
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

// validateCreatePayout проверяет вход CreatePayout по форматам 10.1.
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

// createRequestHash считает стабильный хэш тела создания для идемпотентной
// сверки повторов ключа. Поля кодируются с длиной (length-prefix): склейка
// через разделитель допускала бы коллизии строк с этим разделителем внутри.
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
	sum := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(sum[:])
}

// hasControlRunes сообщает о наличии управляющих символов в строке.
func hasControlRunes(value string) bool {
	return strings.ContainsFunc(value, func(r rune) bool { return r < 0x20 || r == 0x7F })
}

// truncateReason усекает причину до лимита схемы по границе руны:
// разрезанная многобайтовая руна дала бы invalid UTF-8 и отказ БД.
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
