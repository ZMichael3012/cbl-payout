package domain

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/google/uuid"
)

// ErrInvalidAccountPlan сообщает о некорректном бизнес-ключе счета Ledger.
var ErrInvalidAccountPlan = errors.New("payout: invalid account plan key")

// accountNamePrefix задает версионированный префикс канонического имени счета:
// смена схемы вывода идентификаторов выпускается новой версией префикса
// (алгоритм владельца - Ledger 2.6, CBL-003 раздел 9).
const accountNamePrefix = "cbl:ledger:account:v1"

// Правила плана счетов Ledger для актива и ссылки мерчанта: payout обязан
// воспроизводить их один в один, иначе идентификатор счета разойдется.
var (
	planAssetPattern       = regexp.MustCompile(`^[A-Z0-9]{1,16}$`)
	planMerchantRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// MerchantAccountID выводит детерминированный UUIDv5 счета мерчанта
// (merchant/<asset>/<decimals>/<merchant_ref>) по golden-векторам CBL-003 §9:
// одинаковый ключ в payout и Ledger дает один идентификатор (KI-ledger-08).
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
