package domain_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payout/internal/domain"
)

func TestParseApprovalPolicyDefaults(t *testing.T) {
	t.Parallel()

	policy, err := domain.ParseApprovalPolicy("")
	require.NoError(t, err)
	assert.Equal(t, 2, policy.RequiredApprovals("USDT", 1_000_000), "without configuration - two operators (fail-safe)")
	assert.Equal(t, 2, policy.RequiredApprovals("BTC", 1))

	empty := domain.DefaultApprovalPolicy()
	assert.Equal(t, 2, empty.RequiredApprovals("USDT", 5))
	assert.Contains(t, empty.Describe(), "default=2")
}

func TestParseApprovalPolicyTiers(t *testing.T) {
	t.Parallel()

	policy, err := domain.ParseApprovalPolicy(`{"USDT":[{"below_minor":1000000,"approvals":0},{"below_minor":100000000,"approvals":1}],"default":2}`)
	require.NoError(t, err)

	assert.Equal(t, 0, policy.RequiredApprovals("usdt", 999_999), "asset is normalized, first tier")
	assert.Equal(t, 1, policy.RequiredApprovals("USDT", 1_000_000), "tier bound - next tier")
	assert.Equal(t, 1, policy.RequiredApprovals("USDT", 99_999_999))
	assert.Equal(t, 2, policy.RequiredApprovals("USDT", 100_000_000), "above tiers - default")
	assert.Equal(t, 2, policy.RequiredApprovals("BTC", 1), "asset without tiers - default")

	described := policy.Describe()
	assert.Contains(t, described, "USDT=[<1000000:0,<100000000:1]")
	assert.Contains(t, described, "default=2")
}

func TestParseApprovalPolicyInvalid(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"null":                `null`,
		"bad json":            `{`,
		"negative approvals":  `{"USDT":[{"below_minor":10,"approvals":-1}]}`,
		"approvals too big":   `{"USDT":[{"below_minor":10,"approvals":11}]}`,
		"zero below":          `{"USDT":[{"below_minor":0,"approvals":1}]}`,
		"empty tiers":         `{"USDT":[]}`,
		"bad asset":           `{"usdt low":[{"below_minor":10,"approvals":1}]}`,
		"duplicate below":     `{"USDT":[{"below_minor":10,"approvals":1},{"below_minor":10,"approvals":2}]}`,
		"default too big":     `{"default":11}`,
		"default negative":    `{"default":-1}`,
		"duplicate lowercase": `{"USDT":[{"below_minor":10,"approvals":1}],"usdt":[{"below_minor":20,"approvals":1}]}`,
		"null default":        `{"default":null}`,
		"missing approvals":   `{"USDT":[{"below_minor":1000000}]}`,
		"null approvals":      `{"USDT":[{"below_minor":1000000,"approvals":null}]}`,
		"missing below":       `{"USDT":[{"approvals":1}]}`,
		"typo field":          `{"USDT":[{"below_minor":1000000,"approval":2}]}`,
		"unknown extra field": `{"USDT":[{"below_minor":1000000,"approvals":2,"note":"x"}]}`,
		"default extra field": `{"default":{"value":2}}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := domain.ParseApprovalPolicy(raw)
			require.ErrorIs(t, err, domain.ErrInvalidApprovalPolicy)
		})
	}

	// The case of the default key does not matter.
	policy, err := domain.ParseApprovalPolicy(`{"Default":0}`)
	require.NoError(t, err)
	assert.Equal(t, 0, policy.RequiredApprovals("USDT", 1))
}

func TestApprovalPolicyZeroValueGuard(t *testing.T) {
	t.Parallel()

	// The zero struct value means "not configured": fail-safe two operators.
	var unset domain.ApprovalPolicy
	assert.Equal(t, 2, unset.RequiredApprovals("USDT", 1))
	assert.Contains(t, unset.Describe(), "default=2")

	// An explicit default:0 is a legal operator decision.
	explicit, err := domain.ParseApprovalPolicy(`{"default":0}`)
	require.NoError(t, err)
	assert.Equal(t, 0, explicit.RequiredApprovals("USDT", 1))
}

func TestApprovalPolicyDescribeDeterministic(t *testing.T) {
	t.Parallel()

	policy, err := domain.ParseApprovalPolicy(`{"BTC":[{"below_minor":500,"approvals":1}],"usdt":[{"below_minor":100,"approvals":0}]}`)
	require.NoError(t, err)
	first := policy.Describe()
	second := policy.Describe()
	assert.Equal(t, first, second)
	assert.True(t, strings.HasPrefix(first, "BTC=["), "asset sorting is deterministic")
}
