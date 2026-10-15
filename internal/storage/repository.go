package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/cbl/cbl-payout/internal/domain"
)

// Ошибки storage.
var (
	// ErrNotFound сообщает об отсутствии выплаты.
	ErrNotFound = errors.New("payout: payout not found")
	// ErrDuplicateKey сообщает о занятом ключе идемпотентности мерчанта.
	ErrDuplicateKey = errors.New("payout: idempotency key already used")
)

// Payout описывает строку таблицы payouts.
type Payout struct {
	// ID задает UUIDv7 выплаты.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// MerchantID задает мерчанта-владельца выплаты.
	MerchantID string `gorm:"column:merchant_id"`
	// IdempotencyKey задает ключ идемпотентности создания.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash задает хэш тела запроса создания.
	RequestHash string `gorm:"column:request_hash"`
	// State задает текущее состояние автомата.
	State string `gorm:"column:state"`
	// StateVersion задает монотонную версию состояния.
	StateVersion int64 `gorm:"column:state_version"`
	// AmountMinor задает сумму выплаты в минимальных единицах.
	AmountMinor int64 `gorm:"column:amount_minor"`
	// AmountAsset задает актив суммы выплаты.
	AmountAsset string `gorm:"column:amount_asset"`
	// AmountDecimals задает шкалу суммы выплаты.
	AmountDecimals int `gorm:"column:amount_decimals"`
	// FeeMinor задает комиссию в минимальных единицах.
	FeeMinor *int64 `gorm:"column:fee_minor"`
	// FeeAsset задает актив комиссии.
	FeeAsset *string `gorm:"column:fee_asset"`
	// FeeDecimals задает шкалу комиссии.
	FeeDecimals *int `gorm:"column:fee_decimals"`
	// Network задает сеть выплаты.
	Network string `gorm:"column:network"`
	// DestinationAddress задает адрес получателя.
	DestinationAddress string `gorm:"column:destination_address"`
	// MerchantRef задает ссылку мерчанта для корреляции (возвраты 10.6).
	MerchantRef string `gorm:"column:merchant_ref"`
	// ReserveID задает идентификатор удержания Ledger (шаг саги reserved, 10.2).
	ReserveID *string `gorm:"column:reserve_id"`
	// GatewayPayoutID задает идентификатор выплаты в шлюзе.
	GatewayPayoutID *string `gorm:"column:gateway_payout_id"`
	// Provider задает имя провайдера шлюза.
	Provider *string `gorm:"column:provider"`
	// TxID задает хэш транзакции сети.
	TxID *string `gorm:"column:txid"`
	// FailureReason задает причину неудачи (шлюз или оператор).
	FailureReason *string `gorm:"column:failure_reason"`
	// CreatedAt задает время создания.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt задает время последнего перехода.
	UpdatedAt time.Time `gorm:"column:updated_at"`
	// CompletedAt задает время терминального успеха.
	CompletedAt *time.Time `gorm:"column:completed_at"`
}

// TableName задает имя таблицы payouts.
func (Payout) TableName() string { return "payouts" }

// Transition описывает строку журнала переходов payout_transitions.
type Transition struct {
	// ID задает порядковый номер записи журнала.
	ID int64 `gorm:"column:id;primaryKey"`
	// PayoutID задает выплату перехода.
	PayoutID string `gorm:"column:payout_id"`
	// FromState задает состояние до перехода (пусто для создания).
	FromState string `gorm:"column:from_state"`
	// ToState задает состояние после перехода.
	ToState string `gorm:"column:to_state"`
	// StateVersion задает версию состояния после перехода.
	StateVersion int64 `gorm:"column:state_version"`
	// Reason задает причину перехода (событие, запрос, сага).
	Reason string `gorm:"column:reason"`
	// OccurredAt задает время перехода.
	OccurredAt time.Time `gorm:"column:occurred_at"`
}

// TableName задает имя таблицы payout_transitions.
func (Transition) TableName() string { return "payout_transitions" }

// TransitionPatch описывает изменяемые при переходе поля выплаты;
// nil-поля не изменяются.
type TransitionPatch struct {
	// FeeMinor задает комиссию.
	FeeMinor *int64
	// FeeAsset задает актив комиссии.
	FeeAsset *string
	// FeeDecimals задает шкалу комиссии.
	FeeDecimals *int
	// ReserveID задает идентификатор удержания Ledger (фиксируется при резерве).
	ReserveID *string
	// GatewayPayoutID задает идентификатор выплаты в шлюзе.
	GatewayPayoutID *string
	// Provider задает провайдера шлюза.
	Provider *string
	// TxID задает хэш транзакции сети.
	TxID *string
	// FailureReason задает причину неудачи.
	FailureReason *string
	// CompletedAt задает время терминального успеха.
	CompletedAt *time.Time
	// Reason задает причину перехода для журнала.
	Reason string
}

// Repository читает и изменяет выплаты в PostgreSQL.
type Repository struct {
	db        *gorm.DB
	eventSink TransitionEventSink
}

// Option настраивает Repository.
type Option func(*Repository)

// NewRepository создает Repository.
func NewRepository(db *gorm.DB, opts ...Option) *Repository {
	repo := &Repository{db: db}
	for _, opt := range opts {
		opt(repo)
	}
	return repo
}

// Create создает выплату в состоянии requested и записывает переход версии 1
// одной транзакцией. Состояние, версия и данные саги принудительно приводятся
// к начальным: единственная точка создания - автомат (второй барьер - BEFORE
// INSERT триггер). Занятый ключ идемпотентности возвращает ErrDuplicateKey.
func (r *Repository) Create(ctx context.Context, payout Payout) error {
	now := time.Now().UTC()
	payout.State = string(domain.StateRequested)
	payout.StateVersion = 1
	payout.CreatedAt = now
	payout.UpdatedAt = now
	payout.ReserveID = nil
	payout.GatewayPayoutID = nil
	payout.Provider = nil
	payout.TxID = nil
	payout.FailureReason = nil
	payout.CompletedAt = nil
	payout.FeeMinor = nil
	payout.FeeAsset = nil
	payout.FeeDecimals = nil
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if createErr := tx.Create(&payout).Error; createErr != nil {
			if isUniqueViolation(createErr) {
				return fmt.Errorf("%w: merchant %s key %s", ErrDuplicateKey, payout.MerchantID, payout.IdempotencyKey)
			}
			return createErr
		}
		history := Transition{
			PayoutID:     payout.ID,
			FromState:    "",
			ToState:      payout.State,
			StateVersion: 1,
			Reason:       "created",
			OccurredAt:   now,
		}
		if createErr := tx.Create(&history).Error; createErr != nil {
			return createErr
		}
		// Создание - бизнес-веха payout.requested (CF-2): событие пишется
		// в той же транзакции, что и запись выплаты.
		return r.emitTransitionEvent(ctx, tx, TransitionEvent{
			Payout:     payout,
			From:       "",
			To:         domain.StateRequested,
			OccurredAt: now,
		})
	})
	if err != nil {
		return fmt.Errorf("storage: create payout: %w", err)
	}
	return nil
}

// Transition выполняет оптимистичный переход состояния: проверяет матрицу
// домена, обновляет выплату условием state_version = expectedVersion и пишет
// строку журнала. Конфликт версии - domain.ErrStateConflict.
func (r *Repository) Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch TransitionPatch) (Payout, error) {
	return r.TransitionTx(ctx, nil, id, expectedVersion, to, patch)
}

// TransitionTx выполняет переход в переданной транзакции (tx=nil - своя):
// сага 10.2 и consumer 10.4 пишут переходы и outbox-строки атомарно.
func (r *Repository) TransitionTx(ctx context.Context, tx *gorm.DB, id string, expectedVersion uint64, to domain.State, patch TransitionPatch) (Payout, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var updated Payout
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current Payout
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", id).First(&current).Error
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		if findErr != nil {
			return findErr
		}
		if uint64(current.StateVersion) != expectedVersion { //nolint:gosec // state_version ограничен CHECK (>= 1)
			return fmt.Errorf("%w: payout %s version %d, expected %d",
				domain.ErrStateConflict, id, current.StateVersion, expectedVersion)
		}
		from, parseErr := domain.ParseState(current.State)
		if parseErr != nil {
			return parseErr
		}
		newVersion, nextErr := domain.NextVersion(from, to, uint64(current.StateVersion)) //nolint:gosec // state_version ограничен CHECK
		if nextErr != nil {
			return nextErr
		}
		updates := patch.updates()
		updates["state"] = string(to)
		updates["state_version"] = int64(newVersion) //nolint:gosec // рост на 1 от int64-значения БД
		res := tx.Model(&Payout{}).
			Where("id = ? AND state_version = ?", id, expectedVersion).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("%w: payout %s lost race at version %d", domain.ErrStateConflict, id, expectedVersion)
		}
		history := Transition{
			PayoutID:     id,
			FromState:    current.State,
			ToState:      string(to),
			StateVersion: int64(newVersion), //nolint:gosec // рост на 1 от int64-значения БД
			Reason:       patch.Reason,
			OccurredAt:   time.Now().UTC(),
		}
		if createErr := tx.Create(&history).Error; createErr != nil {
			return createErr
		}
		if loadErr := tx.Where("id = ?", id).First(&updated).Error; loadErr != nil {
			return loadErr
		}
		return r.emitTransitionEvent(ctx, tx, TransitionEvent{
			Payout:     updated,
			From:       from,
			To:         to,
			OccurredAt: history.OccurredAt,
		})
	})
	if err != nil {
		return Payout{}, fmt.Errorf("storage: transition payout %s: %w", id, err)
	}
	return updated, nil
}

// PayoutByMerchantKey возвращает выплату мерчанта по ключу идемпотентности:
// повтор CreatePayout сверяет request_hash и возвращает прежний результат.
func (r *Repository) PayoutByMerchantKey(ctx context.Context, merchantID, idempotencyKey string) (Payout, error) {
	var payout Payout
	err := r.db.WithContext(ctx).
		Where("merchant_id = ? AND idempotency_key = ?", merchantID, idempotencyKey).
		First(&payout).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payout{}, fmt.Errorf("%w: merchant %s key %s", ErrNotFound, merchantID, idempotencyKey)
	}
	if err != nil {
		return Payout{}, fmt.Errorf("storage: payout by merchant key: %w", err)
	}
	return payout, nil
}

// Payout возвращает выплату без истории переходов.
func (r *Repository) Payout(ctx context.Context, id string) (Payout, error) {
	return r.PayoutTx(ctx, nil, id)
}

// PayoutTx возвращает выплату без истории переходов в переданной транзакции
// (tx=nil - своя): consumer 10.4 читает состояние в транзакции Guard.
func (r *Repository) PayoutTx(ctx context.Context, tx *gorm.DB, id string) (Payout, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var payout Payout
	err := db.WithContext(ctx).Where("id = ?", id).First(&payout).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payout{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Payout{}, fmt.Errorf("storage: get payout %s: %w", id, err)
	}
	return payout, nil
}

// Get возвращает выплату и ее историю переходов по возрастанию версии.
// Обе выборки выполняются в одной read-only транзакции repeatable read:
// конкурентный переход не должен вернуть timeline новее самой выплаты.
func (r *Repository) Get(ctx context.Context, id string) (Payout, []Transition, error) {
	var payout Payout
	var history []Transition
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if findErr := tx.Where("id = ?", id).First(&payout).Error; findErr != nil {
			return findErr
		}
		return tx.Where("payout_id = ?", id).
			Order("state_version ASC").
			Find(&history).Error
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payout{}, nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Payout{}, nil, fmt.Errorf("storage: get payout %s: %w", id, err)
	}
	return payout, history, nil
}

// updates собирает карту изменений из непустых полей патча.
func (p TransitionPatch) updates() map[string]any {
	updates := make(map[string]any, 8)
	if p.FeeMinor != nil {
		updates["fee_minor"] = *p.FeeMinor
	}
	if p.FeeAsset != nil {
		updates["fee_asset"] = *p.FeeAsset
	}
	if p.FeeDecimals != nil {
		updates["fee_decimals"] = *p.FeeDecimals
	}
	if p.ReserveID != nil {
		updates["reserve_id"] = *p.ReserveID
	}
	if p.GatewayPayoutID != nil {
		updates["gateway_payout_id"] = *p.GatewayPayoutID
	}
	if p.Provider != nil {
		updates["provider"] = *p.Provider
	}
	if p.TxID != nil {
		updates["txid"] = *p.TxID
	}
	if p.FailureReason != nil {
		updates["failure_reason"] = *p.FailureReason
	}
	if p.CompletedAt != nil {
		updates["completed_at"] = *p.CompletedAt
	}
	return updates
}

// isUniqueViolation сообщает, что ошибка БД - занятый ключ идемпотентности
// мерчанта (другие нарушения уникальности не маскируются).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == "payouts_merchant_idempotency_idx"
}
