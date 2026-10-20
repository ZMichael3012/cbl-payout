package usecase_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// fakeLedger replaces the Ledger client and records calls.
type fakeLedger struct {
	reserveErr  error
	commitErr   error
	releaseErr  error
	reserves    []usecase.LedgerReserveRequest
	commits     []string
	commitKeys  []string
	releases    []string
	releaseKeys []string
}

// Reserve records the request or returns an error.
func (f *fakeLedger) Reserve(_ context.Context, req usecase.LedgerReserveRequest) (string, error) {
	f.reserves = append(f.reserves, req)
	if f.reserveErr != nil {
		return "", f.reserveErr
	}
	return "reserve-" + req.RefID, nil
}

// CommitReserve records the commit.
func (f *fakeLedger) CommitReserve(_ context.Context, idempotencyKey, reserveID string) error {
	f.commits = append(f.commits, reserveID)
	f.commitKeys = append(f.commitKeys, idempotencyKey)
	return f.commitErr
}

// ReleaseReserve records the release.
func (f *fakeLedger) ReleaseReserve(_ context.Context, idempotencyKey, reserveID, _ string) error {
	f.releases = append(f.releases, reserveID)
	f.releaseKeys = append(f.releaseKeys, idempotencyKey)
	return f.releaseErr
}

// fakeSagaStore implements usecase.SagaStore and usecase.GatewayEventStore in memory.
type fakeSagaStore struct {
	payouts     map[string]*storage.Payout
	byKey       map[string]string
	commands    []outbox.Envelope
	createErr   error
	notFound    bool
	conflicts   int
	transitions []domain.State
}

// newFakeSagaStore creates an empty store.
func newFakeSagaStore() *fakeSagaStore {
	return &fakeSagaStore{
		payouts: make(map[string]*storage.Payout),
		byKey:   make(map[string]string),
	}
}

// Create creates a payout in memory; a duplicate key is rejected.
func (f *fakeSagaStore) Create(_ context.Context, payout storage.Payout) error {
	if f.createErr != nil {
		return f.createErr
	}
	key := payout.MerchantID + "|" + payout.IdempotencyKey
	if _, exists := f.byKey[key]; exists {
		return storage.ErrDuplicateKey
	}
	payout.State = string(domain.StateRequested)
	payout.StateVersion = 1
	payout.CreatedAt = time.Now().UTC()
	f.payouts[payout.ID] = &payout
	f.byKey[key] = payout.ID
	return nil
}

// Payout returns the payout.
func (f *fakeSagaStore) Payout(_ context.Context, id string) (storage.Payout, error) {
	return f.PayoutTx(context.Background(), nil, id)
}

// PayoutTx returns the payout in a transaction (the fake ignores tx).
func (f *fakeSagaStore) PayoutTx(_ context.Context, _ *gorm.DB, id string) (storage.Payout, error) {
	if f.notFound {
		return storage.Payout{}, storage.ErrNotFound
	}
	payout, ok := f.payouts[id]
	if !ok {
		return storage.Payout{}, storage.ErrNotFound
	}
	return *payout, nil
}

// PayoutByMerchantKey returns a payout by idempotency key.
func (f *fakeSagaStore) PayoutByMerchantKey(_ context.Context, merchantID, key string) (storage.Payout, error) {
	id, ok := f.byKey[merchantID+"|"+key]
	if !ok {
		return storage.Payout{}, storage.ErrNotFound
	}
	return *f.payouts[id], nil
}

// Transaction runs the function and rolls the state back on error.
func (f *fakeSagaStore) Transaction(_ context.Context, fn func(tx *gorm.DB) error) error {
	snapshot := make(map[string]storage.Payout, len(f.payouts))
	for id, payout := range f.payouts {
		snapshot[id] = *payout
	}
	snapshotCommands := len(f.commands)
	snapshotTransitions := len(f.transitions)
	err := fn(nil)
	if err != nil {
		for id, payout := range snapshot {
			copied := payout
			f.payouts[id] = &copied
		}
		f.commands = f.commands[:snapshotCommands]
		f.transitions = f.transitions[:snapshotTransitions]
	}
	return err
}

// PayoutForUpdate returns the payout under the "lock" of the fake.
func (f *fakeSagaStore) PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (storage.Payout, error) {
	return f.PayoutTx(ctx, tx, id)
}

// TransitionTx performs a CAS transition on the fake payout.
func (f *fakeSagaStore) TransitionTx(_ context.Context, _ *gorm.DB, id string, expectedVersion uint64, to domain.State, patch storage.TransitionPatch) (storage.Payout, error) {
	if f.conflicts > 0 {
		f.conflicts--
		return storage.Payout{}, domain.ErrStateConflict
	}
	payout, ok := f.payouts[id]
	if !ok {
		return storage.Payout{}, storage.ErrNotFound
	}
	from, err := domain.ParseState(payout.State)
	if err != nil {
		return storage.Payout{}, err
	}
	next, err := domain.NextVersion(from, to, expectedVersion)
	if err != nil {
		return storage.Payout{}, err
	}
	payout.State = string(to)
	payout.StateVersion = int64(next) //nolint:gosec // test value
	if patch.ReserveID != nil {
		payout.ReserveID = patch.ReserveID
	}
	if patch.FailureReason != nil {
		payout.FailureReason = patch.FailureReason
	}
	if patch.TxID != nil {
		payout.TxID = patch.TxID
	}
	if patch.GatewayPayoutID != nil {
		payout.GatewayPayoutID = patch.GatewayPayoutID
	}
	if patch.Provider != nil {
		payout.Provider = patch.Provider
	}
	if patch.FeeMinor != nil {
		payout.FeeMinor = patch.FeeMinor
		payout.FeeAsset = patch.FeeAsset
		payout.FeeDecimals = patch.FeeDecimals
	}
	if patch.CompletedAt != nil {
		payout.CompletedAt = patch.CompletedAt
	}
	f.transitions = append(f.transitions, to)
	return *payout, nil
}

// AppendOutbox records the command envelope.
func (f *fakeSagaStore) AppendOutbox(_ context.Context, _ *gorm.DB, envelope outbox.Envelope) error {
	f.commands = append(f.commands, envelope)
	return nil
}

// sagaFixture builds the service with fakes.
func sagaFixture(t *testing.T, policyRaw string, ledger usecase.LedgerClient) (*usecase.PayoutService, *fakeSagaStore) {
	t.Helper()
	store := newFakeSagaStore()
	policy, err := domain.ParseApprovalPolicy(policyRaw)
	require.NoError(t, err)
	service := usecase.NewPayoutService(store, ledger, policy, zap.NewNop())
	return service, store
}

// smallPayoutInput sets a valid input of a small payout.
func smallPayoutInput() usecase.CreatePayoutInput {
	return usecase.CreatePayoutInput{
		IdempotencyKey:     "key-1",
		MerchantID:         "m-1",
		AmountMinor:        100_000,
		Asset:              "usdt",
		Decimals:           2,
		Network:            "trc20",
		DestinationAddress: "TJRabPrwbZy45sbavfcjinPJC18kjpRTv8",
		MerchantRef:        "ref-1",
	}
}

func TestCreatePayoutValidation(t *testing.T) {
	t.Parallel()

	service, _ := sagaFixture(t, `{"default":0}`, &fakeLedger{})
	cases := map[string]func(in *usecase.CreatePayoutInput){
		"merchant": func(in *usecase.CreatePayoutInput) { in.MerchantID = "bad id" },
		"key":      func(in *usecase.CreatePayoutInput) { in.IdempotencyKey = "" },
		"amount":   func(in *usecase.CreatePayoutInput) { in.AmountMinor = 0 },
		"asset":    func(in *usecase.CreatePayoutInput) { in.Asset = "u" },
		"decimals": func(in *usecase.CreatePayoutInput) { in.Decimals = 19 },
		"network":  func(in *usecase.CreatePayoutInput) { in.Network = "" },
		"address":  func(in *usecase.CreatePayoutInput) { in.DestinationAddress = "" },
		"ref":      func(in *usecase.CreatePayoutInput) { in.MerchantRef = "line\nbreak" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			in := smallPayoutInput()
			mutate(&in)
			_, err := service.Create(context.Background(), in)
			require.ErrorIs(t, err, usecase.ErrInvalidPayoutInput)
		})
	}
}

func TestCreatePayoutSmallGoesToGateway(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{}
	service, store := sagaFixture(t, `{"USDT":[{"below_minor":1000000,"approvals":0}],"default":2}`, ledger)

	payout, err := service.Create(context.Background(), smallPayoutInput())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateSentToGateway), payout.State)
	assert.Equal(t, int64(3), payout.StateVersion, "requested -> reserved -> sent_to_gateway")

	require.Len(t, ledger.reserves, 1)
	assert.Equal(t, "58686062-c133-5d4e-b08b-0d33a0db924f", ledger.reserves[0].AccountID, "hold account by the golden vector")
	assert.Equal(t, payout.ID, ledger.reserves[0].RefID)
	assert.Equal(t, "payout:"+payout.ID+":reserve", ledger.reserves[0].IdempotencyKey)
	require.NotNil(t, payout.ReserveID)

	require.Len(t, store.commands, 1)
	assert.Equal(t, usecase.TopicPayoutCommands, store.commands[0].Topic)
	assert.Equal(t, payout.ID, store.commands[0].Key)
	assert.Equal(t, "payout.command", store.commands[0].Headers[outbox.HeaderEventType])
}

func TestCreatePayoutLargeWaitsApproval(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{}
	service, store := sagaFixture(t, `{"default":2}`, ledger)

	payout, err := service.Create(context.Background(), smallPayoutInput())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateRequested), payout.State, "a large payout waits for votes")
	assert.Empty(t, ledger.reserves)
	assert.Empty(t, store.commands)
}

func TestCreatePayoutIdempotentReplay(t *testing.T) {
	t.Parallel()

	service, _ := sagaFixture(t, `{"default":0}`, &fakeLedger{})
	in := smallPayoutInput()

	first, err := service.Create(context.Background(), in)
	require.NoError(t, err)

	// A replay with the same body returns the same payout (already in sent_to_gateway).
	second, err := service.Create(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, first.State, second.State)

	// A replay with a different body is a key conflict.
	changed := in
	changed.AmountMinor = 200_000
	_, err = service.Create(context.Background(), changed)
	require.ErrorIs(t, err, usecase.ErrIdempotencyConflict)
}

func TestCreatePayoutRefund(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{}
	service, store := sagaFixture(t, `{"default":0}`, ledger)

	refundInvoice := uuid.Must(uuid.NewV7()).String()
	in := smallPayoutInput()
	in.RefundInvoiceID = strings.ToUpper(refundInvoice) // untrusted case
	payout, err := service.Create(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateSentToGateway), payout.State, "the refund goes along the same saga path")
	require.NotNil(t, payout.RefundInvoiceID)
	assert.Equal(t, refundInvoice, *payout.RefundInvoiceID, "the invoice identifier is canonicalized")

	// A refund and an ordinary payout with the same body but no invoice are different bodies.
	plain := smallPayoutInput()
	plain.IdempotencyKey = in.IdempotencyKey
	_, err = service.Create(context.Background(), plain)
	require.ErrorIs(t, err, usecase.ErrIdempotencyConflict, "the refund marker is part of request_hash")

	// An invalid invoice identifier is rejected before creation.
	for name, badRefund := range map[string]string{
		"not uuid":   "not-a-uuid",
		"nil uuid":   uuid.Nil.String(),
		"whitespace": "   ",
	} {
		bad := smallPayoutInput()
		bad.IdempotencyKey = "refund-bad-" + name
		bad.RefundInvoiceID = badRefund
		_, err = service.Create(context.Background(), bad)
		require.ErrorIsf(t, err, usecase.ErrInvalidPayoutInput, "marker %q", badRefund)
	}
	assert.Len(t, store.payouts, 1, "invalid refunds do not create a payout")
}

func TestCreatePayoutLegacyHashCompatibility(t *testing.T) {
	t.Parallel()

	store := newFakeSagaStore()
	policy, err := domain.ParseApprovalPolicy(`{"default":0}`)
	require.NoError(t, err)
	service := usecase.NewPayoutService(store, &fakeLedger{}, policy, zap.NewNop())

	in := smallPayoutInput()
	// A row created before 10.6: the hash of the format without the refund marker.
	legacyCanonical := "3:m-1" + "6:100000" + "4:USDT" + "1:2" + "5:trc20" +
		"34:TJRabPrwbZy45sbavfcjinPJC18kjpRTv8" + "5:ref-1"
	sum := sha256.Sum256([]byte(legacyCanonical))
	legacyHash := hex.EncodeToString(sum[:])

	id := uuid.Must(uuid.NewV7()).String()
	legacy := storage.Payout{
		ID:                 id,
		MerchantID:         in.MerchantID,
		IdempotencyKey:     in.IdempotencyKey,
		RequestHash:        legacyHash,
		State:              string(domain.StateSentToGateway),
		StateVersion:       3,
		AmountMinor:        in.AmountMinor,
		AmountAsset:        "USDT",
		AmountDecimals:     2,
		Network:            in.Network,
		DestinationAddress: in.DestinationAddress,
		MerchantRef:        in.MerchantRef,
	}
	store.payouts[id] = &legacy
	store.byKey[in.MerchantID+"|"+in.IdempotencyKey] = id

	result, err := service.Create(context.Background(), in)
	require.NoError(t, err, "the hash of an ordinary payout before 10.6 matches: the replay is idempotent")
	assert.Equal(t, id, result.ID)
}

func TestCreatePayoutHashSeparatesFieldBoundaries(t *testing.T) {
	t.Parallel()

	service, _ := sagaFixture(t, `{"default":0}`, &fakeLedger{})
	first := smallPayoutInput()
	first.Network = "trc20|extra"
	first.DestinationAddress = "Taddr"
	_, err := service.Create(context.Background(), first)
	require.NoError(t, err)

	// The same bytes at the field boundary but a different split: network
	// "trc20", address "extra|Taddr". The hash must distinguish the bodies
	// (otherwise a foreign address would come back as an idempotent replay).
	second := smallPayoutInput()
	second.Network = "trc20"
	second.DestinationAddress = "extra|Taddr"
	_, err = service.Create(context.Background(), second)
	require.ErrorIs(t, err, usecase.ErrIdempotencyConflict)
}

func TestCreatePayoutLedgerRejectedCompensates(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{reserveErr: usecase.ErrLedgerRejected}
	service, store := sagaFixture(t, `{"default":0}`, ledger)

	payout, err := service.Create(context.Background(), smallPayoutInput())
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompensated), payout.State, "rejection before the reserve: failed -> compensated")
	require.NotNil(t, payout.FailureReason)
	assert.Equal(t, []domain.State{domain.StateFailed, domain.StateCompensated}, store.transitions)
	assert.Empty(t, ledger.releases, "release is not called without a reserve")
}

func TestCreatePayoutLedgerUnavailableKeepsState(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{reserveErr: usecase.ErrLedgerUnavailable}
	service, store := sagaFixture(t, `{"default":0}`, ledger)

	_, err := service.Create(context.Background(), smallPayoutInput())
	require.ErrorIs(t, err, usecase.ErrLedgerUnavailable)

	// The payout stayed in requested: a Create or Advance replay completes the saga.
	for _, payout := range store.payouts {
		assert.Equal(t, string(domain.StateRequested), payout.State)
	}
	assert.Empty(t, store.commands)
}

func TestAdvanceIdempotent(t *testing.T) {
	t.Parallel()

	ledger := &fakeLedger{}
	service, _ := sagaFixture(t, `{"default":0}`, ledger)
	payout, err := service.Create(context.Background(), smallPayoutInput())
	require.NoError(t, err)

	again, err := service.Advance(context.Background(), payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateSentToGateway), again.State)
	assert.Len(t, ledger.reserves, 1, "a repeated Advance does not reserve again")
}

func TestAdvanceWithoutLedgerUnavailable(t *testing.T) {
	t.Parallel()

	store := newFakeSagaStore()
	policy, err := domain.ParseApprovalPolicy(`{"default":0}`)
	require.NoError(t, err)
	service := usecase.NewPayoutService(store, nil, policy, zap.NewNop())

	in := smallPayoutInput()
	in.MerchantID = "m-2"
	payout := storage.Payout{ID: uuid.Must(uuid.NewV7()).String(), MerchantID: "m-2", IdempotencyKey: "k", RequestHash: "h",
		AmountMinor: 100, AmountAsset: "USDT", AmountDecimals: 2, Network: "trc20", DestinationAddress: "T"}
	require.NoError(t, store.Create(context.Background(), payout))

	_, err = service.Advance(context.Background(), payout.ID)
	require.ErrorIs(t, err, usecase.ErrSagaUnavailable)
}

func TestGetValidatesID(t *testing.T) {
	t.Parallel()

	service, _ := sagaFixture(t, `{"default":0}`, &fakeLedger{})
	_, err := service.Get(context.Background(), "not-a-uuid")
	require.ErrorIs(t, err, usecase.ErrInvalidPayoutInput)
}
