package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalidApprovalPolicy сообщает о некорректной конфигурации политики апрува.
var ErrInvalidApprovalPolicy = errors.New("payout: invalid approval policy")

// Границы политики апрува.
const (
	// defaultRequiredApprovals задает число операторов без настройки: fail-safe,
	// без политики выплата не проходит без двух подтверждений.
	defaultRequiredApprovals = 2
	// maxRequiredApprovals задает верхнюю границу числа операторов.
	maxRequiredApprovals = 10
)

// ApprovalTier задает ярус требований апрува по сумме актива.
// Поля-указатели отделяют отсутствие значения от явного нуля: пропущенный
// `approvals` или `null` - ошибка конфигурации, а не тихое «апрув не нужен».
type ApprovalTier struct {
	// BelowMinor задает верхнюю границу яруса (не включительно) в минимальных единицах.
	BelowMinor *int64 `json:"below_minor"`
	// Approvals задает требуемое число операторов яруса.
	Approvals *int `json:"approvals"`
}

// ApprovalPolicy задает политику двухэтапного подтверждения: сколько
// операторов требуется по активу и сумме выплаты. Нулевое значение структуры
// не сконфигурировано и трактуется как fail-safe (два оператора).
type ApprovalPolicy struct {
	tiers      map[string][]ApprovalTier
	fallback   int
	configured bool
}

// DefaultApprovalPolicy создает политику по умолчанию: два оператора.
func DefaultApprovalPolicy() ApprovalPolicy {
	return ApprovalPolicy{fallback: defaultRequiredApprovals, configured: true}
}

// ParseApprovalPolicy разбирает JSON-конфигурацию требований апрува.
// Пустая строка - политика по умолчанию; ярусы сортируются по сумме.
// Неизвестные поля ярусов и `null` отклоняются: опечатка в конфигурации
// апрува не должна молча отключать ручное подтверждение выплат.
func ParseApprovalPolicy(raw string) (ApprovalPolicy, error) {
	policy := ApprovalPolicy{fallback: defaultRequiredApprovals, configured: true}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return policy, nil
	}
	if raw == "null" {
		return ApprovalPolicy{}, fmt.Errorf("%w: null config", ErrInvalidApprovalPolicy)
	}
	var generic map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &generic); err != nil {
		return ApprovalPolicy{}, fmt.Errorf("%w: %w", ErrInvalidApprovalPolicy, err)
	}
	policy.tiers = make(map[string][]ApprovalTier, len(generic))
	seenAssets := make(map[string]struct{}, len(generic))
	for name, value := range generic {
		if strings.ToLower(strings.TrimSpace(name)) == "default" {
			def, err := strictDecode[int](value)
			if err != nil {
				return ApprovalPolicy{}, fmt.Errorf("%w: default: %w", ErrInvalidApprovalPolicy, err)
			}
			if def < 0 || def > maxRequiredApprovals {
				return ApprovalPolicy{}, fmt.Errorf("%w: default %d", ErrInvalidApprovalPolicy, def)
			}
			policy.fallback = def
			continue
		}
		var tiers []ApprovalTier
		if err := strictDecodeInto(value, &tiers); err != nil {
			return ApprovalPolicy{}, fmt.Errorf("%w: asset %q: %w", ErrInvalidApprovalPolicy, name, err)
		}
		if len(tiers) == 0 {
			return ApprovalPolicy{}, fmt.Errorf("%w: asset %q has no tiers", ErrInvalidApprovalPolicy, name)
		}
		asset := strings.ToUpper(strings.TrimSpace(name))
		if !validApprovalAsset(asset) {
			return ApprovalPolicy{}, fmt.Errorf("%w: asset %q", ErrInvalidApprovalPolicy, name)
		}
		if _, dup := seenAssets[asset]; dup {
			return ApprovalPolicy{}, fmt.Errorf("%w: duplicate asset %q", ErrInvalidApprovalPolicy, name)
		}
		seenAssets[asset] = struct{}{}
		sort.Slice(tiers, func(i, j int) bool { return *tiers[i].BelowMinor < *tiers[j].BelowMinor })
		seen := make(map[int64]struct{}, len(tiers))
		for _, tier := range tiers {
			if tier.BelowMinor == nil {
				return ApprovalPolicy{}, fmt.Errorf("%w: asset %q missing below_minor", ErrInvalidApprovalPolicy, name)
			}
			if tier.Approvals == nil {
				return ApprovalPolicy{}, fmt.Errorf("%w: asset %q missing approvals", ErrInvalidApprovalPolicy, name)
			}
			if *tier.BelowMinor <= 0 {
				return ApprovalPolicy{}, fmt.Errorf("%w: asset %q below_minor %d", ErrInvalidApprovalPolicy, name, *tier.BelowMinor)
			}
			if *tier.Approvals < 0 || *tier.Approvals > maxRequiredApprovals {
				return ApprovalPolicy{}, fmt.Errorf("%w: asset %q approvals %d", ErrInvalidApprovalPolicy, name, *tier.Approvals)
			}
			if _, dup := seen[*tier.BelowMinor]; dup {
				return ApprovalPolicy{}, fmt.Errorf("%w: asset %q duplicate below_minor %d", ErrInvalidApprovalPolicy, name, *tier.BelowMinor)
			}
			seen[*tier.BelowMinor] = struct{}{}
		}
		policy.tiers[asset] = tiers
	}
	return policy, nil
}

// strictDecode разбирает JSON-значение, запрещая неизвестные поля и null.
func strictDecode[T any](raw json.RawMessage) (T, error) {
	var value T
	if err := strictDecodeInto(raw, &value); err != nil {
		return value, err
	}
	return value, nil
}

// strictDecodeInto разбирает JSON в цель, запрещая неизвестные поля и null:
// json.Decoder молча игнорирует null для скалярных целей.
func strictDecodeInto(raw json.RawMessage, target any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("null value")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("trailing JSON data")
	}
	return nil
}

// RequiredApprovals возвращает требуемое число операторов для актива и суммы:
// первый ярус, где сумма строго меньше границы, иначе fallback. Нулевое
// значение политики (не сконфигурирована) защищено fail-safe значением.
func (p ApprovalPolicy) RequiredApprovals(asset string, amountMinor int64) int {
	fallback := p.fallback
	if !p.configured {
		fallback = defaultRequiredApprovals
	}
	tiers, ok := p.tiers[strings.ToUpper(asset)]
	if !ok {
		return fallback
	}
	for _, tier := range tiers {
		if tier.BelowMinor != nil && amountMinor < *tier.BelowMinor {
			if tier.Approvals == nil {
				return fallback
			}
			return *tier.Approvals
		}
	}
	return fallback
}

// Describe возвращает детерминированное описание эффективной политики для
// лога конфигурации при старте (без секретов).
func (p ApprovalPolicy) Describe() string {
	assets := make([]string, 0, len(p.tiers))
	for asset := range p.tiers {
		assets = append(assets, asset)
	}
	sort.Strings(assets)
	parts := make([]string, 0, len(assets)+1)
	for _, asset := range assets {
		tiers := p.tiers[asset]
		bounds := make([]string, 0, len(tiers))
		for _, tier := range tiers {
			below, approvals := int64(0), 0
			if tier.BelowMinor != nil {
				below = *tier.BelowMinor
			}
			if tier.Approvals != nil {
				approvals = *tier.Approvals
			}
			bounds = append(bounds, fmt.Sprintf("<%d:%d", below, approvals))
		}
		parts = append(parts, asset+"=["+strings.Join(bounds, ",")+"]")
	}
	fallback := p.fallback
	if !p.configured {
		fallback = defaultRequiredApprovals
	}
	parts = append(parts, fmt.Sprintf("default=%d", fallback))
	return strings.Join(parts, ";")
}

// validApprovalAsset проверяет код актива политики: 2 - 16 символов латиницы и цифр.
func validApprovalAsset(asset string) bool {
	if len(asset) < 2 || len(asset) > 16 {
		return false
	}
	for _, r := range asset {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}
