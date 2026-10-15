package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-payout/internal/domain"
)

// TestMerchantAccountGoldenVectors проверяет вывод UUIDv5 счета удержания по
// golden-векторам CBL-003 §9: расхождение с Ledger недопустимо (KI-ledger-08).
func TestMerchantAccountGoldenVectors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ref      string
		asset    string
		decimals int
		want     string
	}{
		{"m-1", "USDT", 2, "58686062-c133-5d4e-b08b-0d33a0db924f"},
		{"m-1", "BTC", 8, "e1a671a1-a662-5305-b144-6e623099ea46"},
		{"M-1_a", "USDT", 2, "00f2dc5b-a0c1-51c1-b2eb-f261e0ed538f"},
	}
	for _, tc := range cases {
		id, err := domain.MerchantAccountID(tc.ref, tc.asset, tc.decimals)
		require.NoError(t, err)
		assert.Equal(t, tc.want, id, "ключ merchant/%s/%d/%s", tc.asset, tc.decimals, tc.ref)
	}
}

func TestMerchantAccountIDInvalid(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		ref      string
		asset    string
		decimals int
	}{
		"empty ref":     {"", "USDT", 2},
		"colon in ref":  {"m:1", "USDT", 2},
		"space in ref":  {"m 1", "USDT", 2},
		"lowercase ass": {"m-1", "usdt", 2},
		"empty asset":   {"m-1", "", 2},
		"decimals > 18": {"m-1", "USDT", 19},
		"decimals < 0":  {"m-1", "USDT", -1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := domain.MerchantAccountID(tc.ref, tc.asset, tc.decimals)
			require.ErrorIs(t, err, domain.ErrInvalidAccountPlan)
		})
	}
}
