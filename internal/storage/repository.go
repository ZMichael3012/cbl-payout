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

// Storage errors.
var (
	// ErrNotFound reports a missing payout.
	ErrNotFound = errors.New("payout: payout not found")
	// ErrDuplicateKey reports a taken merchant idempotency key.
	ErrDuplicateKey = errors.New("payout: idempotency key already used")
)

// Payout describes a row of the payouts table.
type Payout struct {
	// ID sets the payout UUIDv7.
	ID string `gorm:"column:id;type:uuid;primaryKey"`
	// MerchantID sets the merchant that owns the payout.
	MerchantID string `gorm:"column:merchant_id"`
	// IdempotencyKey sets the creation idempotency key.
	IdempotencyKey string `gorm:"column:idempotency_key"`
	// RequestHash sets the hash of the creation request body.
	RequestHash string `gorm:"column:request_hash"`
	// State sets the current state machine state.
	State string `gorm:"column:state"`
	// StateVersion sets the monotonic state version.
	StateVersion int64 `gorm:"column:state_version"`
	// AmountMinor sets the payout amount in minor units.
	AmountMinor int64 `gorm:"column:amount_minor"`
	// AmountAsset sets the asset of the payout amount.
	AmountAsset string `gorm:"column:amount_asset"`
	// AmountDecimals sets the scale of the payout amount.
	AmountDecimals int `gorm:"column:amount_decimals"`
	// FeeMinor sets the fee in minor units.
	FeeMinor *int64 `gorm:"column:fee_minor"`
	// FeeAsset sets the fee asset.
	FeeAsset *string `gorm:"column:fee_asset"`
	// FeeDecimals sets the fee scale.
	FeeDecimals *int `gorm:"column:fee_decimals"`
	// Network sets the payout network.
	Network string `gorm:"column:network"`
	// DestinationAddress sets the recipient address.
	DestinationAddress string `gorm:"column:destination_address"`
	// MerchantRef sets the merchant reference for correlation (refunds 10.6).
	MerchantRef string `gorm:"column:merchant_ref"`
	// RefundInvoiceID sets the source refund invoice: a non-empty value
	// means the payout is a refund (CBL-10.6).
	RefundInvoiceID *string `gorm:"column:refund_invoice_id"`
	// ReserveID sets the Ledger hold identifier (saga step reserved, 10.2).
	ReserveID *string `gorm:"column:reserve_id"`
	// GatewayPayoutID sets the payout identifier in the gateway.
	GatewayPayoutID *string `gorm:"column:gateway_payout_id"`
	// Provider sets the gateway provider name.
	Provider *string `gorm:"column:provider"`
	// TxID sets the network transaction hash.
	TxID *string `gorm:"column:txid"`
	// FailureReason sets the failure reason (gateway or operator).
	FailureReason *string `gorm:"column:failure_reason"`
	// CreatedAt sets the creation time.
	CreatedAt time.Time `gorm:"column:created_at"`
	// UpdatedAt sets the time of the last transition.
	UpdatedAt time.Time `gorm:"column:updated_at"`
	// CompletedAt sets the time of terminal success.
	CompletedAt *time.Time `gorm:"column:completed_at"`
}

// TableName sets the payouts table name.
func (Payout) TableName() string { return "payouts" }

// Transition describes a row of the payout_transitions journal.
type Transition struct {
	// ID sets the sequential journal record number.
	ID int64 `gorm:"column:id;primaryKey"`
	// PayoutID sets the payout of the transition.
	PayoutID string `gorm:"column:payout_id"`
	// FromState sets the state before the transition (empty for creation).
	FromState string `gorm:"column:from_state"`
	// ToState sets the state after the transition.
	ToState string `gorm:"column:to_state"`
	// StateVersion sets the state version after the transition.
	StateVersion int64 `gorm:"column:state_version"`
	// Reason sets the transition reason (event, request, saga).
	Reason string `gorm:"column:reason"`
	// OccurredAt sets the transition time.
	OccurredAt time.Time `gorm:"column:occurred_at"`
}

// TableName sets the payout_transitions table name.
func (Transition) TableName() string { return "payout_transitions" }

// TransitionPatch describes the payout fields changed during a transition;
// nil fields are not changed.
type TransitionPatch struct {
	// FeeMinor sets the fee.
	FeeMinor *int64
	// FeeAsset sets the fee asset.
	FeeAsset *string
	// FeeDecimals sets the fee scale.
	FeeDecimals *int
	// ReserveID sets the Ledger hold identifier (fixed on reserve).
	ReserveID *string
	// GatewayPayoutID sets the payout identifier in the gateway.
	GatewayPayoutID *string
	// Provider sets the gateway provider.
	Provider *string
	// TxID sets the network transaction hash.
	TxID *string
	// FailureReason sets the failure reason.
	FailureReason *string
	// CompletedAt sets the time of terminal success.
	CompletedAt *time.Time
	// Reason sets the transition reason for the journal.
	Reason string
}

// Repository reads and modifies payouts in PostgreSQL.
type Repository struct {
	db        *gorm.DB
	eventSink TransitionEventSink
}

// Option configures Repository.
type Option func(*Repository)

// NewRepository creates a Repository.
func NewRepository(db *gorm.DB, opts ...Option) *Repository {
	repo := &Repository{db: db}
	for _, opt := range opts {
		opt(repo)
	}
	return repo
}

// Create creates a payout in the requested state and records the version 1
// transition in a single transaction. The state, version and saga data are
// forced to their initial values: the state machine is the only creation
// point (the second barrier is the BEFORE INSERT trigger). A taken
// idempotency key returns ErrDuplicateKey.
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
		// Creation is the payout.requested business milestone (CF-2): the
		// event is written in the same transaction as the payout record.
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

// Transition performs an optimistic state transition: it checks the domain
// matrix, updates the payout with the state_version = expectedVersion
// condition and writes a journal row. A version conflict is
// domain.ErrStateConflict.
func (r *Repository) Transition(ctx context.Context, id string, expectedVersion uint64, to domain.State, patch TransitionPatch) (Payout, error) {
	return r.TransitionTx(ctx, nil, id, expectedVersion, to, patch)
}

// TransitionTx performs the transition in the passed transaction (tx=nil
// means its own): saga 10.2 and consumer 10.4 write transitions and outbox
// rows atomically.
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
		if uint64(current.StateVersion) != expectedVersion { //nolint:gosec // state_version is constrained by CHECK (>= 1)
			return fmt.Errorf("%w: payout %s version %d, expected %d",
				domain.ErrStateConflict, id, current.StateVersion, expectedVersion)
		}
		from, parseErr := domain.ParseState(current.State)
		if parseErr != nil {
			return parseErr
		}
		newVersion, nextErr := domain.NextVersion(from, to, uint64(current.StateVersion)) //nolint:gosec // state_version is constrained by CHECK
		if nextErr != nil {
			return nextErr
		}
		updates := patch.updates()
		updates["state"] = string(to)
		updates["state_version"] = int64(newVersion) //nolint:gosec // grows by 1 from an int64 DB value
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
			StateVersion: int64(newVersion), //nolint:gosec // grows by 1 from an int64 DB value
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

// PayoutByMerchantKey returns a merchant payout by idempotency key: a
// repeated CreatePayout compares request_hash and returns the previous
// result.
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

// Payout returns a payout without transition history.
func (r *Repository) Payout(ctx context.Context, id string) (Payout, error) {
	return r.PayoutTx(ctx, nil, id)
}

// PayoutTx returns a payout without transition history in the passed
// transaction (tx=nil means its own): consumer 10.4 reads the state in the
// Guard transaction.
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

// Get returns a payout and its transition history in ascending version order.
// Both queries run in a single read-only repeatable read transaction: a
// concurrent transition must not return a timeline newer than the payout
// itself.
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

// updates builds the change map from non-nil patch fields.
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

// isUniqueViolation reports that the DB error is a taken merchant idempotency
// key (other uniqueness violations are not masked).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == "payouts_merchant_idempotency_idx"
}
