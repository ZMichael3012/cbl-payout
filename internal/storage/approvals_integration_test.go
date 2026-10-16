//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// TestIntegrationApprovalRollbackOnConflict checks on a real DB that a
// conflicting attempt (CAS) rolls back an already recorded operator vote: the
// approval service errApprovalRetry relies on transactionality.
func TestIntegrationApprovalRollbackOnConflict(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payout := newPayout(t, "approval-rollback-1")
	require.NoError(t, repo.Create(ctx, payout))
	payout.StateVersion = 1 // Create fixes version 1

	staleVersion := uint64(payout.StateVersion) + 1
	err := repo.Transaction(ctx, func(tx *gorm.DB) error {
		if _, recordErr := repo.RecordApproval(ctx, tx, storage.Approval{
			PayoutID:   payout.ID,
			OperatorID: "op-1",
			Approve:    true,
			Required:   2,
		}); recordErr != nil {
			return recordErr
		}
		// A stale version: the conflict rolls back the whole transaction.
		_, transitionErr := repo.TransitionTx(ctx, tx, payout.ID, staleVersion, domain.StateApproved, storage.TransitionPatch{})
		return transitionErr
	})
	require.ErrorIs(t, err, domain.ErrStateConflict)

	var approvals int64
	require.NoError(t, db.Table("payout_approvals").Where("payout_id = ?", payout.ID).Count(&approvals).Error)
	assert.Zero(t, approvals, "the vote is rolled back with the conflicting transaction")

	// Recording the vote again after the rollback succeeds.
	ok, err := repo.RecordApproval(ctx, nil, storage.Approval{
		PayoutID:   payout.ID,
		OperatorID: "op-1",
		Approve:    true,
		Required:   2,
	})
	require.NoError(t, err)
	assert.True(t, ok)
}

// TestIntegrationApprovalOperatorConstraints checks the vote journal CHECKs.
func TestIntegrationApprovalOperatorConstraints(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payout := newPayout(t, "approval-check-1")
	require.NoError(t, repo.Create(ctx, payout))

	badOperator := db.Exec(`INSERT INTO payout_approvals (payout_id, operator_id, approve, required)
		VALUES (?, 'bad op', true, 2)`, payout.ID)
	require.Error(t, badOperator.Error, "spaces in operator_id are rejected by the CHECK")

	controlReason := db.Exec(`INSERT INTO payout_approvals (payout_id, operator_id, approve, reason, required)
		VALUES (?, 'op-1', false, 'line' || chr(10) || 'break', 2)`, payout.ID)
	require.Error(t, controlReason.Error, "control characters in reason are rejected by the CHECK")

	zeroRequired := db.Exec(`INSERT INTO payout_approvals (payout_id, operator_id, approve, required)
		VALUES (?, 'op-2', true, 0)`, payout.ID)
	require.Error(t, zeroRequired.Error, "required=0 is invalid: a vote without a requirement is not written")
}
