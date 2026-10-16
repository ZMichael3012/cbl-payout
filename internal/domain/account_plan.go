package domain

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// ErrInvalidAccountPlan reports an invalid Ledger account business key.
var ErrInvalidAccountPlan = errors.New("payout: invalid account plan key")

// accountNamePrefix defines the versioned prefix of the canonical account name:
// a change of the identifier derivation scheme is issued as a new prefix version
// (owner algorithm - Ledger 2.6, CBL-003 section 9).
const accountNamePrefix = "cbl:ledger:account:v1"

// Ledger account plan rules for the asset and the merchant reference: payout must
// reproduce them one to one, otherwise the account identifier would diverge.
var (
	planAssetPattern       = regexp.MustCompile(`^[A-Z0-9]{1,16}$`)
	planMerchantRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// MerchantAccountID derives the deterministic UUIDv5 of the merchant account
// (merchant/<asset>/<decimals>/<merchant_ref>) from the golden vectors of CBL-003 section 9:
// the same key in payout and Ledger yields one identifier (KI-ledger-08).
func MerchantAccountID(merchantRef, asset string, decimals int) (string, error) {
	if !planAssetPattern.MatchString(asset) {
		return "", fmt.Errorf("%w: asset %q", ErrInvalidAccountPlan, asset)
	}
	if decimals < 0 || decimals > 18 {
		return "", fmt.Errorf("%w: decimals %d", ErrInvalidAccountPlan, decimals)
	}
	if !planMerchantRefPattern.MatchString(merchantRef) {
		return "", fmt.Errorf("%w: merchant ref %q", ErrInvalidAccountPlan, merchantRef)
	}
	name := fmt.Sprintf("%s:merchant:%s:%d:%s", accountNamePrefix, asset, decimals, merchantRef)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String(), nil
}
