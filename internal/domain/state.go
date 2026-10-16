package domain

import (
	"errors"
	"fmt"
)

// Payout state machine errors.
var (
	// ErrInvalidState reports an unknown state.
	ErrInvalidState = errors.New("payout: invalid state")
	// ErrForbiddenTransition reports a transition forbidden by the matrix.
	ErrForbiddenTransition = errors.New("payout: forbidden transition")
	// ErrTerminalState reports an attempt to change a terminal state.
	ErrTerminalState = errors.New("payout: terminal state is immutable")
	// ErrStateConflict reports a state version conflict (optimistic lock).
	ErrStateConflict = errors.New("payout: state version conflict")
)

// State defines a payout state machine state.
type State string

// Canonical states match the billing.payout.v1.PayoutState values
// (CF-1): the names match the enum suffixes in lowercase.
const (
	// StateRequested means the request is created, limits and approval are being processed.
	StateRequested State = "requested"
	// StateApproved means the operator approval passed (large payouts, 10.5).
	StateApproved State = "approved"
	// StateReserved means the Ledger hold is created (saga step, 10.2).
	StateReserved State = "reserved"
	// StateSentToGateway means the command is sent to the Connector (payout.commands).
	StateSentToGateway State = "sent_to_gateway"
	// StateCompleted means the payout is confirmed by the gateway.
	StateCompleted State = "completed"
	// StateFailed means an external step failed (compensation ahead).
	StateFailed State = "failed"
	// StateCompensated means the saga is closed: the hold is released or was not required.
	StateCompensated State = "compensated"
	// StateRejected means the payout is rejected by an operator before the reserve.
	StateRejected State = "rejected"
)

// transitions defines the matrix of allowed state machine transitions: the single
// source of rules for the application and the basis for the database second barrier (10.1).
var transitions = map[State]map[State]struct{}{
	StateRequested: {
		StateApproved: {},
		StateReserved: {},
		StateRejected: {},
		StateFailed:   {},
	},
	StateApproved: {
		StateReserved: {},
		StateRejected: {},
		StateFailed:   {},
	},
	StateReserved: {
		StateSentToGateway: {},
		StateFailed:        {},
	},
	StateSentToGateway: {
		StateCompleted: {},
		StateFailed:    {},
	},
	StateFailed: {
		StateCompensated: {},
	},
}

// terminalStates defines the states without outgoing transitions.
var terminalStates = map[State]struct{}{
	StateCompleted:   {},
	StateCompensated: {},
	StateRejected:    {},
}

// ParseState parses a string into a state machine state.
func ParseState(raw string) (State, error) {
	state := State(raw)
	if _, ok := transitions[state]; ok {
		return state, nil
	}
	if _, ok := terminalStates[state]; ok {
		return state, nil
	}
	return "", fmt.Errorf("%w: %q", ErrInvalidState, raw)
}

// AllowedTransition reports whether the transition is allowed by the matrix.
func AllowedTransition(from, to State) bool {
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// Terminal reports whether the state is terminal.
func Terminal(state State) bool {
	_, ok := terminalStates[state]
	return ok
}

// NextVersion validates the transition and returns the new state version.
// A self-transition (saga data update: gateway_payout_id, fee) is allowed
// only in non-terminal states; terminal states are immutable.
func NextVersion(from, to State, version uint64) (uint64, error) {
	if version == 0 {
		return 0, fmt.Errorf("%w: zero version", ErrStateConflict)
	}
	if _, err := ParseState(string(from)); err != nil {
		return 0, err
	}
	if _, err := ParseState(string(to)); err != nil {
		return 0, err
	}
	if Terminal(from) {
		return 0, fmt.Errorf("%w: %s", ErrTerminalState, from)
	}
	if from == to {
		return version + 1, nil
	}
	if !AllowedTransition(from, to) {
		return 0, fmt.Errorf("%w: %s -> %s", ErrForbiddenTransition, from, to)
	}
	return version + 1, nil
}
