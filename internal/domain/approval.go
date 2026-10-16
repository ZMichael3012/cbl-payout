package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ErrInvalidApprovalPolicy reports an invalid approval policy configuration.
var ErrInvalidApprovalPolicy = errors.New("payout: invalid approval policy")

// Approval policy bounds.
const (
	// defaultRequiredApprovals defines the number of operators without configuration: fail-safe,
	// without a policy a payout does not pass without two confirmations.
	defaultRequiredApprovals = 2
	// maxRequiredApprovals defines the upper bound of the operator count.
	maxRequiredApprovals = 10
)

// ApprovalTier defines the approval requirement tier by asset amount.
// Pointer fields separate a missing value from an explicit zero: a missing
// `approvals` or `null` is a configuration error, not a silent "approval not required".
type ApprovalTier struct {
	// BelowMinor defines the tier upper bound (exclusive) in minor units.
	BelowMinor *int64 `json:"below_minor"`
	// Approvals defines the required number of operators of the tier.
	Approvals *int `json:"approvals"`
}

// ApprovalPolicy defines the two-step confirmation policy: how many
// operators are required per asset and payout amount. The zero value of the struct
// is not configured and is treated as fail-safe (two operators).
type ApprovalPolicy struct {
	tiers      map[string][]ApprovalTier
	fallback   int
	configured bool
}

// DefaultApprovalPolicy creates the default policy: two operators.
func DefaultApprovalPolicy() ApprovalPolicy {
	return ApprovalPolicy{fallback: defaultRequiredApprovals, configured: true}
}

// ParseApprovalPolicy parses the JSON configuration of approval requirements.
// An empty string means the default policy; tiers are sorted by amount.
// Unknown tier fields and `null` are rejected: a typo in the approval
// configuration must not silently disable manual payout confirmation.
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

// strictDecode parses a JSON value, rejecting unknown fields and null.
func strictDecode[T any](raw json.RawMessage) (T, error) {
	var value T
	if err := strictDecodeInto(raw, &value); err != nil {
		return value, err
	}
	return value, nil
}

// strictDecodeInto parses JSON into the target, rejecting unknown fields and null:
// json.Decoder silently ignores null for scalar targets.
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

// RequiredApprovals returns the required number of operators for the asset and amount:
// the first tier where the amount is strictly below the bound, otherwise the fallback.
// The zero policy value (not configured) is guarded by the fail-safe value.
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

// Describe returns a deterministic description of the effective policy for the
// startup configuration log (without secrets).
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

// validApprovalAsset validates the policy asset code: 2 to 16 Latin letters and digits.
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
