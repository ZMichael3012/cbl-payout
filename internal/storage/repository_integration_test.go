//go:build integration

package storage_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/ZMichael3012/cbl-kit/storagex"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// allStates repeats the canonical state list for pair enumeration.
var allStates = []domain.State{
	domain.StateRequested,
	domain.StateApproved,
	domain.StateReserved,
	domain.StateSentToGateway,
	domain.StateCompleted,
	domain.StateFailed,
	domain.StateCompensated,
	domain.StateRejected,
}

func setupPayoutDB(t *testing.T) *gorm.DB {
	t.Helper()
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("payout"),
		tcpostgres.WithUsername("cbl"),
		tcpostgres.WithPassword("cbl"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	require.NoError(t, storagex.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	return db
}

// newPayout returns a valid payout in the requested state.
func newPayout(t *testing.T, key string) storage.Payout {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	return storage.Payout{
		ID:                 id.String(),
		MerchantID:         "m-1",
		IdempotencyKey:     key,
		RequestHash:        "hash-" + key,
		AmountMinor:        100_000,
		AmountAsset:        "USDT",
		AmountDecimals:     2,
		Network:            "trc20",
		DestinationAddress: "TJRabPrwbZy45sbavfcjinPJC18kjpRTv8",
		MerchantRef:        "ref-" + key,
	}
}

// transition performs a transition and returns the updated payout.
func transition(t *testing.T, repo *storage.Repository, id string, version uint64, to domain.State, patch storage.TransitionPatch) storage.Payout {
	t.Helper()
	payout, err := repo.Transition(context.Background(), id, version, to, patch)
	require.NoError(t, err)
	return payout
}

// payoutAtState drives a new payout through the chain of states.
func payoutAtState(t *testing.T, repo *storage.Repository, key string, chain ...domain.State) storage.Payout {
	t.Helper()
	payout := newPayout(t, key)
	require.NoError(t, repo.Create(context.Background(), payout))
	current := payout
	current.State = string(domain.StateRequested)
	current.StateVersion = 1
	for _, state := range chain {
		patch := storage.TransitionPatch{}
		if state == domain.StateCompleted {
			// completed requires completed_at (CHECK): the caller must set it.
			completedAt := time.Now().UTC()
			patch.CompletedAt = &completedAt
		}
		current = transition(t, repo, payout.ID, uint64(current.StateVersion), state, patch)
	}
	return current
}

func TestIntegrationCreateAndHistory(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	payout := newPayout(t, "key-1")
	require.NoError(t, repo.Create(context.Background(), payout))

	loaded, history, err := repo.Get(context.Background(), payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), loaded.State)
	assert.Equal(t, int64(1), loaded.StateVersion)
	assert.Equal(t, int64(100_000), loaded.AmountMinor)
	assert.Equal(t, "USDT", loaded.AmountAsset)
	require.Len(t, history, 1)
	assert.Equal(t, "", history[0].FromState)
	assert.Equal(t, string(domain.StateRequested), history[0].ToState)
	assert.Equal(t, int64(1), history[0].StateVersion)

	_, _, err = repo.Get(context.Background(), uuid.Must(uuid.NewV7()).String())
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestIntegrationCreateForcesInitialState(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	forced := newPayout(t, "forced-1")
	forced.State = string(domain.StateCompleted)
	forced.StateVersion = 7
	require.NoError(t, repo.Create(context.Background(), forced))
	loaded, history, err := repo.Get(context.Background(), forced.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), loaded.State)
	assert.Equal(t, int64(1), loaded.StateVersion)
	require.Len(t, history, 1)

	// A direct INSERT bypassing the state machine is rejected by the BEFORE INSERT trigger.
	rawID, err := uuid.NewV7()
	require.NoError(t, err)
	raw := db.Exec(`INSERT INTO payouts (id, merchant_id, idempotency_key, request_hash, state, state_version,
		amount_minor, amount_asset, amount_decimals, network, destination_address, merchant_ref)
		VALUES (?, 'm-1', 'raw-1', 'hash-raw', 'approved', 7, 100, 'USDT', 2, 'trc20', 'Taddr', '')`, rawID.String())
	require.Error(t, raw.Error)
	assert.Contains(t, raw.Error.Error(), "requested")
}

func TestIntegrationDuplicateIdempotencyKey(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	first := newPayout(t, "same-key")
	require.NoError(t, repo.Create(context.Background(), first))

	second := newPayout(t, "same-key")
	err := repo.Create(context.Background(), second)
	require.ErrorIs(t, err, storage.ErrDuplicateKey)
}

func TestIntegrationTransitionChain(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	completedAt := time.Now().UTC().Truncate(time.Millisecond)
	gatewayID := "gw-1"
	txid := "tx-1"
	payout := payoutAtState(t, repo, "chain-1", domain.StateReserved, domain.StateSentToGateway)
	payout = transition(t, repo, payout.ID, uint64(payout.StateVersion), domain.StateCompleted, storage.TransitionPatch{
		GatewayPayoutID: &gatewayID,
		TxID:            &txid,
		CompletedAt:     &completedAt,
	})

	loaded, history, err := repo.Get(context.Background(), payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), loaded.State)
	assert.Equal(t, int64(4), loaded.StateVersion)
	require.NotNil(t, loaded.CompletedAt)
	assert.WithinDuration(t, completedAt, *loaded.CompletedAt, time.Millisecond)
	require.Len(t, history, 4)
	assert.Equal(t, []string{"requested", "reserved", "sent_to_gateway", "completed"},
		[]string{history[0].ToState, history[1].ToState, history[2].ToState, history[3].ToState})
}

func TestIntegrationCompensationChain(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	reason := "gateway rejected address"
	payout := payoutAtState(t, repo, "comp-1", domain.StateApproved, domain.StateReserved)
	payout = transition(t, repo, payout.ID, uint64(payout.StateVersion), domain.StateFailed, storage.TransitionPatch{
		FailureReason: &reason,
	})
	payout = transition(t, repo, payout.ID, uint64(payout.StateVersion), domain.StateCompensated, storage.TransitionPatch{})

	loaded, history, err := repo.Get(context.Background(), payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompensated), loaded.State)
	require.NotNil(t, loaded.FailureReason)
	assert.Equal(t, reason, *loaded.FailureReason)
	assert.Len(t, history, 5, "requested, approved, reserved, failed, compensated")
}

func TestIntegrationConcurrentTransitionOnlyOneWins(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	payout := payoutAtState(t, repo, "race-1")
	version := uint64(payout.StateVersion)

	results := make(chan error, 2)
	for _, target := range []domain.State{domain.StateReserved, domain.StateRejected} {
		target := target
		go func() {
			_, err := repo.Transition(context.Background(), payout.ID, version, target, storage.TransitionPatch{})
			results <- err
		}()
	}
	first, second := <-results, <-results
	conflicts := 0
	for _, err := range []error{first, second} {
		if err != nil {
			require.ErrorIs(t, err, domain.ErrStateConflict)
			conflicts++
		}
	}
	assert.Equal(t, 1, conflicts, "exactly one transition wins the race")

	_, history, err := repo.Get(context.Background(), payout.ID)
	require.NoError(t, err)
	assert.Len(t, history, 2)
}

func TestIntegrationStaleVersionRejected(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payout := payoutAtState(t, repo, "stale-1", domain.StateReserved)
	_, err := repo.Transition(ctx, payout.ID, 1, domain.StateSentToGateway, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrStateConflict, "a stale version is not applied")

	loaded, history, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateReserved), loaded.State)
	assert.Len(t, history, 2, "the journal does not grow on conflict")
}

func TestIntegrationForbiddenAndTerminalTransitions(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payout := payoutAtState(t, repo, "forbid-1", domain.StateReserved, domain.StateSentToGateway)
	_, err := repo.Transition(ctx, payout.ID, uint64(payout.StateVersion), domain.StateReserved, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrForbiddenTransition, "a backward transition is forbidden")

	completed := payoutAtState(t, repo, "forbid-2", domain.StateReserved, domain.StateSentToGateway, domain.StateCompleted)
	_, err = repo.Transition(ctx, completed.ID, uint64(completed.StateVersion), domain.StateFailed, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrTerminalState)

	rejected := payoutAtState(t, repo, "forbid-3", domain.StateRejected)
	_, err = repo.Transition(ctx, rejected.ID, uint64(rejected.StateVersion), domain.StateReserved, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrTerminalState)
}

func TestIntegrationSelfTransitionUpdatable(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	gatewayID := "gw-9"
	payout := payoutAtState(t, repo, "self-1", domain.StateReserved, domain.StateSentToGateway)
	payout = transition(t, repo, payout.ID, uint64(payout.StateVersion), domain.StateSentToGateway, storage.TransitionPatch{
		GatewayPayoutID: &gatewayID,
	})
	assert.Equal(t, int64(4), payout.StateVersion)
	require.NotNil(t, payout.GatewayPayoutID)

	// A self-transition writes a journal row with equal from/to.
	_, history, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	require.Len(t, history, 4)
	last := history[3]
	assert.Equal(t, string(domain.StateSentToGateway), last.FromState)
	assert.Equal(t, string(domain.StateSentToGateway), last.ToState)
	assert.Equal(t, int64(4), last.StateVersion)

	completed := payoutAtState(t, repo, "self-2", domain.StateReserved, domain.StateSentToGateway, domain.StateCompleted)
	_, err = repo.Transition(ctx, completed.ID, uint64(completed.StateVersion), domain.StateCompleted, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrTerminalState, "a self-transition in a terminal state is forbidden")
}

func TestIntegrationReserveIDFixedOnReserve(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	reserveID := "0193a0e2-1111-7aaa-bbbb-000000000001"
	payout := payoutAtState(t, repo, "reserve-1")
	payout = transition(t, repo, payout.ID, uint64(payout.StateVersion), domain.StateReserved, storage.TransitionPatch{
		ReserveID: &reserveID,
	})
	require.NotNil(t, payout.ReserveID)
	assert.Equal(t, reserveID, *payout.ReserveID)

	// The hold identifier is immutable once fixed (the second barrier).
	res := db.Exec("UPDATE payouts SET reserve_id = 'other', state_version = state_version + 1 WHERE id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "reserve_id")
}

func TestIntegrationDatabaseTriggerSecondBarrier(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	payout := payoutAtState(t, repo, "barrier-1")

	// A direct UPDATE bypassing CAS: the version is not incremented.
	res := db.Exec("UPDATE payouts SET state = 'reserved' WHERE id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "state_version")

	// A forbidden pair with a correct version increment.
	res = db.Exec("UPDATE payouts SET state = 'completed', state_version = state_version + 1, completed_at = now() WHERE id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "forbidden")

	// Immutable fields (all of them, including the recipient details).
	columns := map[string]string{
		"amount_minor":        "1",
		"amount_asset":        "'BTC'",
		"amount_decimals":     "8",
		"network":             "'erc20'",
		"destination_address": "'0xattacker'",
		"merchant_ref":        "'other'",
		"idempotency_key":     "'other'",
	}
	for column, value := range columns {
		res = db.Exec(fmt.Sprintf("UPDATE payouts SET %s = %s, state_version = state_version + 1 WHERE id = ?", column, value), payout.ID) //nolint:gosec // the column name comes from a local test map, the value is a constant
		require.Errorf(t, res.Error, "field %s is immutable", column)
		assert.Contains(t, res.Error.Error(), "immutable")
	}

	// Saga data does not appear on an INSERT that bypasses transitions.
	rawSagaID, err := uuid.NewV7()
	require.NoError(t, err)
	res = db.Exec(`INSERT INTO payouts (id, merchant_id, idempotency_key, request_hash, state, state_version,
		amount_minor, amount_asset, amount_decimals, network, destination_address, gateway_payout_id)
		VALUES (?, 'm-1', 'raw-saga', 'hash', 'requested', 1, 100, 'USDT', 2, 'trc20', 'Taddr', 'gw-pre')`, rawSagaID.String())
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "saga fields")
}

func TestIntegrationTerminalStateImmutableInDatabase(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	payout := payoutAtState(t, repo, "term-1", domain.StateRejected)
	res := db.Exec("UPDATE payouts SET state = 'rejected', state_version = state_version + 1 WHERE id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "immutable")
}

func TestIntegrationTransitionsAppendOnly(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	payout := payoutAtState(t, repo, "append-1", domain.StateReserved)

	res := db.Exec("UPDATE payout_transitions SET reason = 'rewritten' WHERE payout_id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "append-only")

	res = db.Exec("DELETE FROM payout_transitions WHERE payout_id = ?", payout.ID)
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "append-only")

	res = db.Exec("TRUNCATE payout_transitions")
	require.Error(t, res.Error)
	assert.Contains(t, res.Error.Error(), "TRUNCATE")
}

func TestIntegrationTransitionsDictionary(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	payout := newPayout(t, "dict-1")
	require.NoError(t, repo.Create(context.Background(), payout))

	fake := db.Exec(`INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version)
		VALUES (?, '', 'nonsense', 2)`, payout.ID)
	require.Error(t, fake.Error, "an unknown state does not pass the journal dictionary")
}

func TestIntegrationConstraints(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	zero := newPayout(t, "zero-1")
	zero.AmountMinor = 0
	require.Error(t, repo.Create(ctx, zero), "a zero amount is rejected")

	negative := newPayout(t, "negative-1")
	negative.AmountMinor = -5
	require.Error(t, repo.Create(ctx, negative), "a negative amount is rejected")

	badDecimals := newPayout(t, "decimals-1")
	badDecimals.AmountDecimals = 19
	require.Error(t, repo.Create(ctx, badDecimals), "decimals outside 0 - 18 are rejected")

	badAsset := newPayout(t, "asset-1")
	badAsset.AmountAsset = "usdt"
	require.Error(t, repo.Create(ctx, badAsset), "a lowercase asset is rejected")

	emptyAsset := newPayout(t, "asset-2")
	emptyAsset.AmountAsset = ""
	require.Error(t, repo.Create(ctx, emptyAsset), "an empty asset is rejected")

	emptyNetwork := newPayout(t, "network-1")
	emptyNetwork.Network = ""
	require.Error(t, repo.Create(ctx, emptyNetwork), "an empty network is rejected")

	emptyAddress := newPayout(t, "address-1")
	emptyAddress.DestinationAddress = ""
	require.Error(t, repo.Create(ctx, emptyAddress), "an empty destination address is rejected")

	longKey := newPayout(t, "long-key-1")
	longKey.IdempotencyKey = strings.Repeat("k", 256)
	require.Error(t, repo.Create(ctx, longKey), "an idempotency key longer than 255 is rejected")

	longAddress := newPayout(t, "long-address-1")
	longAddress.DestinationAddress = strings.Repeat("T", 257)
	require.Error(t, repo.Create(ctx, longAddress), "an address longer than 256 is rejected")

	payout := payoutAtState(t, repo, "fee-1", domain.StateReserved)
	negativeFee := int64(-1)
	_, err := repo.Transition(ctx, payout.ID, uint64(payout.StateVersion), domain.StateSentToGateway, storage.TransitionPatch{
		FeeMinor: &negativeFee,
	})
	require.Error(t, err, "a negative fee is rejected by the CHECK")

	// A partial fee triple (amount without asset/scale) does not form Money.
	partialFee := int64(50)
	_, err = repo.Transition(ctx, payout.ID, uint64(payout.StateVersion), domain.StateSentToGateway, storage.TransitionPatch{
		FeeMinor: &partialFee,
	})
	require.Error(t, err, "a partial fee triple is rejected by the CHECK")

	// The journal from_state dictionary: an unknown state is rejected.
	fake := db.Exec(`INSERT INTO payout_transitions (payout_id, from_state, to_state, state_version)
		VALUES (?, 'nonsense', 'reserved', 2)`, payout.ID)
	require.Error(t, fake.Error, "an unknown from_state does not pass the journal dictionary")
}

func TestIntegrationCompletedAtOnlyCompleted(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	completedAt := time.Now().UTC()
	payout := payoutAtState(t, repo, "done-1", domain.StateReserved)
	_, err := repo.Transition(ctx, payout.ID, uint64(payout.StateVersion), domain.StateFailed, storage.TransitionPatch{
		CompletedAt: &completedAt,
	})
	require.Error(t, err, "completed_at is allowed only in completed")

	// completed without completed_at is rejected by the CHECK: the terminal
	// success time is mandatory for analytics and reconciliation.
	noTime := payoutAtState(t, repo, "done-2", domain.StateReserved, domain.StateSentToGateway)
	_, err = repo.Transition(ctx, noTime.ID, uint64(noTime.StateVersion), domain.StateCompleted, storage.TransitionPatch{})
	require.Error(t, err, "completed without completed_at is rejected")

	ok := payoutAtState(t, repo, "done-3", domain.StateReserved, domain.StateSentToGateway)
	updated := transition(t, repo, ok.ID, uint64(ok.StateVersion), domain.StateCompleted, storage.TransitionPatch{CompletedAt: &completedAt})
	require.NotNil(t, updated.CompletedAt)

	compensated := payoutAtState(t, repo, "done-4", domain.StateFailed)
	_, err = repo.Transition(ctx, compensated.ID, uint64(compensated.StateVersion), domain.StateCompensated, storage.TransitionPatch{
		CompletedAt: &completedAt,
	})
	require.Error(t, err, "compensated is not a completed_at state")

	// A successful compensation, then a repeat is a terminal refusal.
	compensated, err = repo.Transition(ctx, compensated.ID, uint64(compensated.StateVersion), domain.StateCompensated, storage.TransitionPatch{})
	require.NoError(t, err)
	_, err = repo.Transition(ctx, compensated.ID, uint64(compensated.StateVersion), domain.StateCompensated, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrTerminalState)

	// A repeated failed -> compensated on a stale version is a conflict.
	failedAgain := payoutAtState(t, repo, "done-5", domain.StateFailed)
	stale := uint64(failedAgain.StateVersion) - 1
	_, err = repo.Transition(ctx, failedAgain.ID, stale, domain.StateCompensated, storage.TransitionPatch{})
	require.ErrorIs(t, err, domain.ErrStateConflict)
}

func TestIntegrationPayoutTxReadsInTransaction(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payout := payoutAtState(t, repo, "pty-1", domain.StateReserved)
	loaded, err := repo.PayoutTx(ctx, nil, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateReserved), loaded.State)

	_, err = repo.PayoutTx(ctx, nil, uuid.Must(uuid.NewV7()).String())
	require.ErrorIs(t, err, storage.ErrNotFound)

	// Reading in the passed transaction: the consumer 10.4 path.
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		inner, txErr := repo.PayoutTx(ctx, tx, payout.ID)
		require.NoError(t, txErr)
		assert.Equal(t, payout.ID, inner.ID)
		return nil
	}))
}

func TestIntegrationTransitionTxRollbackAtomic(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)
	ctx := context.Background()

	payout := payoutAtState(t, repo, "tx-1")

	// A failure of the outer transaction rolls back the transition with it:
	// the saga 10.2 writes the outbox row in exactly such a transaction.
	sentinel := errors.New("saga step failed")
	err := db.Transaction(func(tx *gorm.DB) error {
		if _, txErr := repo.TransitionTx(ctx, tx, payout.ID, uint64(payout.StateVersion), domain.StateReserved, storage.TransitionPatch{}); txErr != nil {
			return txErr
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	loaded, history, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), loaded.State, "the transition is rolled back with the transaction")
	assert.Len(t, history, 1)

	// A successful outer transaction commits the transition.
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		_, txErr := repo.TransitionTx(ctx, tx, payout.ID, uint64(payout.StateVersion), domain.StateReserved, storage.TransitionPatch{})
		return txErr
	}))
	loaded, _, err = repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateReserved), loaded.State)
}

// TestIntegrationTriggerMirrorsMatrix enumerates all state pairs against the
// DB trigger: pairs allowed by the matrix (and self-transitions in
// non-terminal states) must pass, the rest must be rejected. It catches drift
// between the Go matrix and SQL.
func TestIntegrationTriggerMirrorsMatrix(t *testing.T) {
	db := setupPayoutDB(t)
	repo := storage.NewRepository(db)

	chains := map[domain.State][]domain.State{
		domain.StateRequested:     {},
		domain.StateApproved:      {domain.StateApproved},
		domain.StateReserved:      {domain.StateReserved},
		domain.StateSentToGateway: {domain.StateReserved, domain.StateSentToGateway},
		domain.StateCompleted:     {domain.StateReserved, domain.StateSentToGateway, domain.StateCompleted},
		domain.StateFailed:        {domain.StateFailed},
		domain.StateCompensated:   {domain.StateFailed, domain.StateCompensated},
		domain.StateRejected:      {domain.StateRejected},
	}
	index := 0
	for _, from := range allStates {
		for _, to := range allStates {
			index++
			payout := payoutAtState(t, repo, fmt.Sprintf("mx-%d", index), chains[from]...)
			require.Equalf(t, string(from), payout.State, "the chain drives to %s", from)
			// completed requires completed_at (CHECK): the raw-SQL path must set it.
			sql := "UPDATE payouts SET state = ?, state_version = state_version + 1 WHERE id = ?"
			if to == domain.StateCompleted {
				sql = "UPDATE payouts SET state = ?, state_version = state_version + 1, completed_at = now() WHERE id = ?"
			}
			res := db.Exec(sql, string(to), payout.ID)
			want := domain.AllowedTransition(from, to) || (from == to && !domain.Terminal(from))
			if want {
				require.NoErrorf(t, res.Error, "the SQL matrix must allow %s -> %s", from, to)
			} else {
				require.Errorf(t, res.Error, "the SQL matrix must forbid %s -> %s", from, to)
			}
		}
	}
}
