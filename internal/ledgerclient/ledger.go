// Package ledgerclient implements the Ledger gRPC client for the payout saga:
// reserve, commit and release of the hold. Business rejections are separated from
// transport errors: the former complete the payout with compensation, the latter are retried.
package ledgerclient

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/ledger/v1"

	"github.com/cbl/cbl-payout/internal/usecase"
)

// timeout defines the deadline of Ledger calls (CF-1: reserve within the CreatePayout budget).
const timeout = time.Second

// Client implements usecase.LedgerClient over the gRPC LedgerService.
type Client struct {
	conn *grpc.ClientConn
	svc  ledgerv1.LedgerServiceClient
}

// Dial creates a connection to Ledger (without mTLS until 0.5/0.6; service calls
// are part of the trusted network perimeter, KI-ledger-02/KI-payout-02).
func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("ledgerclient: dial %s: %w", target, err)
	}
	return &Client{conn: conn, svc: ledgerv1.NewLedgerServiceClient(conn)}, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Reserve creates a hold and returns its identifier.
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

// CommitReserve turns the hold into a posting.
func (c *Client) CommitReserve(ctx context.Context, idempotencyKey, reserveID string) error {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := c.svc.CommitReserve(callCtx, &ledgerv1.CommitReserveRequest{
		IdempotencyKey: idempotencyKey,
		ReserveId:      reserveID,
	})
	return classify(err)
}

// ReleaseReserve releases the hold (compensation of a failed payout).
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

// classify maps a gRPC error to the saga contract: a business rejection
// (precondition/invalid/not found) versus transport unavailability.
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

// decimalsToUint32 safely converts the scale to the contract uint32.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
