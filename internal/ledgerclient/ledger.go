// Пакет ledgerclient реализует gRPC-клиент Ledger для саги выплат: резерв,
// коммит и release удержания. Бизнес-отказы отделены от транспортных ошибок:
// первые завершают выплату компенсацией, вторые - повторяются.
package ledgerclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-payout/internal/usecase"
)

// timeout задает дедлайн вызовов Ledger (CF-1: резерв в бюджете CreatePayout).
const timeout = time.Second

// Client реализует usecase.LedgerClient поверх gRPC LedgerService.
type Client struct {
	conn *grpc.ClientConn
	svc  ledgerv1.LedgerServiceClient
}

// Dial создает подключение к Ledger (без mTLS до 0.5/0.6; сервисные вызовы -
// часть периметра доверенной сети, KI-ledger-02/KI-payout-02).
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("ledgerclient: dial %s: %w", target, err)
	}
	return &Client{conn: conn, svc: ledgerv1.NewLedgerServiceClient(conn)}, nil
}

// Close закрывает подключение.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Reserve создает удержание и возвращает его идентификатор.
func (c *Client) Reserve(ctx context.Context, req usecase.LedgerReserveRequest) (string, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := c.svc.Reserve(callCtx, &ledgerv1.ReserveRequest{
		IdempotencyKey: req.IdempotencyKey,
		AccountId:      req.AccountID,
		Amount: &commonv1.Money{
			MinorUnits: req.AmountMinor,
			Asset:      req.Asset,
			Decimals:   decimalsToUint32(req.Decimals),
		},
		RefId: req.RefID,
	})
	if err != nil {
		return "", classify(err)
	}
	if response.GetReserveId() == "" {
		return "", fmt.Errorf("%w: empty reserve id", usecase.ErrLedgerUnavailable)
	}
	return response.GetReserveId(), nil
}

// CommitReserve превращает удержание в проводку.
func (c *Client) CommitReserve(ctx context.Context, idempotencyKey, reserveID string) error {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := c.svc.CommitReserve(callCtx, &ledgerv1.CommitReserveRequest{
		IdempotencyKey: idempotencyKey,
		ReserveId:      reserveID,
	})
	return classify(err)
}

// ReleaseReserve освобождает удержание (компенсация неудачной выплаты).
func (c *Client) ReleaseReserve(ctx context.Context, idempotencyKey, reserveID, reason string) error {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := c.svc.ReleaseReserve(callCtx, &ledgerv1.ReleaseReserveRequest{
		IdempotencyKey: idempotencyKey,
		ReserveId:      reserveID,
		Reason:         reason,
	})
	return classify(err)
}

// classify переводит gRPC-ошибку в договоренности саги: бизнес-отказ
// (precondition/invalid/not found) против транспортной недоступности.
func classify(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.AlreadyExists:
		return fmt.Errorf("%w: %w", usecase.ErrLedgerRejected, err)
	default:
		return fmt.Errorf("%w: %w", usecase.ErrLedgerUnavailable, err)
	}
}

// decimalsToUint32 безопасно переводит шкалу в uint32 контракта.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
