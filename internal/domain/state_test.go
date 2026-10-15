package domain_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	payoutv1 "github.com/cbl/cbl-protos/gen/go/billing/payout/v1"

	"github.com/cbl/cbl-payout/internal/domain"
)

// allStates задает канонические состояния автомата выплаты.
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

// allowedPairs задает матрицу переходов (FR-3) в виде пар "from>to".
var allowedPairs = map[string]struct{}{
	"requested>approved":        {},
	"requested>reserved":        {},
	"requested>rejected":        {},
	"requested>failed":          {},
	"approved>reserved":         {},
	"approved>rejected":         {},
	"approved>failed":           {},
	"reserved>sent_to_gateway":  {},
	"reserved>failed":           {},
	"sent_to_gateway>completed": {},
	"sent_to_gateway>failed":    {},
	"failed>compensated":        {},
}

func TestParseState(t *testing.T) {
	t.Parallel()

	for _, state := range allStates {
		parsed, err := domain.ParseState(string(state))
		require.NoErrorf(t, err, "состояние %s", state)
		assert.Equal(t, state, parsed)
	}
	_, err := domain.ParseState("unknown")
	require.ErrorIs(t, err, domain.ErrInvalidState)
	_, err = domain.ParseState("")
	require.ErrorIs(t, err, domain.ErrInvalidState)
}

func TestTransitionMatrix(t *testing.T) {
	t.Parallel()

	for _, from := range allStates {
		for _, to := range allStates {
			_, want := allowedPairs[string(from)+">"+string(to)]
			assert.Equalf(t, want, domain.AllowedTransition(from, to),
				"переход %s -> %s", from, to)
		}
	}
}

func TestTerminal(t *testing.T) {
	t.Parallel()

	terminal := []domain.State{domain.StateCompleted, domain.StateCompensated, domain.StateRejected}
	for _, state := range terminal {
		assert.Truef(t, domain.Terminal(state), "%s должен быть терминальным", state)
	}
	active := []domain.State{
		domain.StateRequested, domain.StateApproved, domain.StateReserved,
		domain.StateSentToGateway, domain.StateFailed,
	}
	for _, state := range active {
		assert.Falsef(t, domain.Terminal(state), "%s не терминальный", state)
	}
}

func TestNextVersion(t *testing.T) {
	t.Parallel()

	version, err := domain.NextVersion(domain.StateRequested, domain.StateReserved, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), version)

	version, err = domain.NextVersion(domain.StateSentToGateway, domain.StateSentToGateway, 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), version, "самопереход увеличивает версию")

	_, err = domain.NextVersion(domain.StateSentToGateway, domain.StateReserved, 5)
	require.ErrorIs(t, err, domain.ErrForbiddenTransition, "переход назад запрещен")

	_, err = domain.NextVersion(domain.StateCompensated, domain.StateCompensated, 5)
	require.ErrorIs(t, err, domain.ErrTerminalState, "терминальное состояние неизменяемо")

	_, err = domain.NextVersion(domain.StateRequested, domain.StateCompleted, 2)
	require.ErrorIs(t, err, domain.ErrForbiddenTransition, "перескок через состояния запрещен")

	_, err = domain.NextVersion(domain.StateFailed, domain.StateCompensated, 0)
	require.ErrorIs(t, err, domain.ErrStateConflict)

	_, err = domain.NextVersion("unknown", "unknown", 1)
	require.ErrorIs(t, err, domain.ErrInvalidState, "неизвестное состояние не проходит самопереходом")
}

func TestStateMatchesProtoEnum(t *testing.T) {
	t.Parallel()

	protoStates := make(map[string]struct{})
	for _, name := range payoutv1.PayoutState_name {
		if strings.HasSuffix(name, "_UNSPECIFIED") {
			continue
		}
		protoStates[strings.ToLower(strings.TrimPrefix(name, "PAYOUT_STATE_"))] = struct{}{}
	}
	require.Len(t, protoStates, len(allStates))
	for _, state := range allStates {
		_, ok := protoStates[string(state)]
		assert.Truef(t, ok, "состояние %s отсутствует в billing.payout.v1.PayoutState", state)
	}
}
