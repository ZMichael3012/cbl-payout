package domain

import (
	"errors"
	"fmt"
)

// Ошибки автомата состояний выплаты.
var (
	// ErrInvalidState сообщает о неизвестном состоянии.
	ErrInvalidState = errors.New("payout: invalid state")
	// ErrForbiddenTransition сообщает о переходе, запрещенном матрицей.
	ErrForbiddenTransition = errors.New("payout: forbidden transition")
	// ErrTerminalState сообщает о попытке изменить терминальное состояние.
	ErrTerminalState = errors.New("payout: terminal state is immutable")
	// ErrStateConflict сообщает о конфликте версии состояния (оптимистичная блокировка).
	ErrStateConflict = errors.New("payout: state version conflict")
)

// State задает состояние конечного автомата выплаты.
type State string

// Канонические состояния соответствуют значениям billing.payout.v1.PayoutState
// (CF-1): имена совпадают по нижнему регистру с суффиксами enum.
const (
	// StateRequested - заявка создана, лимиты и апрув обрабатываются.
	StateRequested State = "requested"
	// StateApproved - апрув оператора пройден (крупные выплаты, 10.5).
	StateApproved State = "approved"
	// StateReserved - удержание в Ledger создано (шаг саги, 10.2).
	StateReserved State = "reserved"
	// StateSentToGateway - команда отправлена в Connector (payout.commands).
	StateSentToGateway State = "sent_to_gateway"
	// StateCompleted - выплата подтверждена шлюзом.
	StateCompleted State = "completed"
	// StateFailed - внешний шаг завершился неудачей (компенсация впереди).
	StateFailed State = "failed"
	// StateCompensated - сага закрыта: удержание освобождено или не требовалось.
	StateCompensated State = "compensated"
	// StateRejected - выплата отклонена оператором до резерва.
	StateRejected State = "rejected"
)

// transitions задает матрицу разрешенных переходов автомата: единственный
// источник правил для приложения и основа для второго барьера БД (10.1).
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

// terminalStates задает состояния без исходящих переходов.
var terminalStates = map[State]struct{}{
	StateCompleted:   {},
	StateCompensated: {},
	StateRejected:    {},
}

// ParseState разбирает строку в состояние автомата.
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

// AllowedTransition сообщает, разрешен ли переход матрицей.
func AllowedTransition(from, to State) bool {
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// Terminal сообщает, является ли состояние терминальным.
func Terminal(state State) bool {
	_, ok := terminalStates[state]
	return ok
}

// NextVersion проверяет переход и возвращает новую версию состояния.
// Самопереход (обновление данных саги: gateway_payout_id, fee) допустим
// только в нетерминальных состояниях; терминальные состояния неизменяемы.
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
