package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Approval describes a row of the payout_approvals operator vote journal.
type Approval struct {
	// ID sets the sequential journal record number.
	ID int64 `gorm:"column:id;primaryKey"`
	// PayoutID sets the payout being voted on.
	PayoutID string `gorm:"column:payout_id"`
	// OperatorID sets the operator who cast the vote.
	OperatorID string `gorm:"column:operator_id"`
	// Approve sets the operator decision (false means rejection).
	Approve bool `gorm:"column:approve"`
	// Reason sets the decision reason (required in spirit for a rejection).
	Reason string `gorm:"column:reason"`
	// Required sets the required operator count at vote time (policy audit).
	Required int `gorm:"column:required"`
	// CreatedAt sets the vote time.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName sets the payout_approvals table name.
func (Approval) TableName() string { return "payout_approvals" }

// Transaction runs the function in a repository transaction: an operator vote
// is recorded atomically with a possible state transition (10.5). Errors are
// returned unwrapped: the caller distinguishes business errors via errors.Is.
func (r *Repository) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
	if fn == nil {
		return errors.New("storage: nil transaction function")
	}
	return r.db.WithContext(ctx).Transaction(fn)
}

// PayoutForUpdate returns the payout under a row lock in the passed
// transaction: concurrent operator votes are serialized and the counter does
// not diverge.
func (r *Repository) PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (Payout, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var payout Payout
	err := db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", id).First(&payout).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payout{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Payout{}, fmt.Errorf("storage: lock payout %s: %w", id, err)
	}
	return payout, nil
}

// RecordApproval appends an operator vote; a repeated vote by the same
// operator is idempotent (unique (payout_id, operator_id)) and returns false.
func (r *Repository) RecordApproval(ctx context.Context, tx *gorm.DB, approval Approval) (bool, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	approval.CreatedAt = time.Now().UTC()
	res := db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "payout_id"}, {Name: "operator_id"}},
			DoNothing: true,
		}).
		Create(&approval)
	if res.Error != nil {
		return false, fmt.Errorf("storage: record approval: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
}

// ApprovalByOperator returns the operator vote for a payout (if any): the
// approval service compares a repeated decision with the stored one
// (append-only).
func (r *Repository) ApprovalByOperator(ctx context.Context, tx *gorm.DB, payoutID, operatorID string) (Approval, bool, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var approval Approval
	err := db.WithContext(ctx).
		Where("payout_id = ? AND operator_id = ?", payoutID, operatorID).
		First(&approval).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Approval{}, false, nil
	}
	if err != nil {
		return Approval{}, false, fmt.Errorf("storage: approval by operator: %w", err)
	}
	return approval, true, nil
}

// ApprovalCount returns the number of votes for a payout.
func (r *Repository) ApprovalCount(ctx context.Context, tx *gorm.DB, payoutID string) (int, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var count int64
	err := db.WithContext(ctx).Model(&Approval{}).
		Where("payout_id = ?", payoutID).
		Count(&count).Error
	if err != nil {
		return 0, fmt.Errorf("storage: approval count: %w", err)
	}
	return int(count), nil
}

// ListApprovals returns the operators whose approval votes were recorded, in
// vote order: the administrative read contour shows the approval progress
// (CBL-12.2, additive Payout.approved_by); a rejecting vote is not an approval
// and stays out of the list.
func (r *Repository) ListApprovals(ctx context.Context, payoutID string) ([]string, error) {
	var operators []string
	err := r.db.WithContext(ctx).Model(&Approval{}).
		Where("payout_id = ? AND approve = ?", payoutID, true).
		Order("created_at ASC, id ASC").
		Pluck("operator_id", &operators).Error
	if err != nil {
		return nil, fmt.Errorf("storage: list approvals: %w", err)
	}
	return operators, nil
}
