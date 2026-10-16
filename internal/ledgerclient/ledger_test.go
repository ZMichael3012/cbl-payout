package ledgerclient

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cbl/cbl-payout/internal/usecase"
)

// TestClassify separates Ledger business rejections from transport unavailability:
// the former complete the payout with compensation, the latter are retried.
func TestClassify(t *testing.T) {
	t.Parallel()

	business := []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.AlreadyExists}
	for _, code := range business {
		err := classify(status.Error(code, "rejected"))
		require.ErrorIsf(t, err, usecase.ErrLedgerRejected, "code %s", code)
		assert.Falsef(t, errors.Is(err, usecase.ErrLedgerUnavailable), "code %s", code)
	}
	transient := []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal, codes.Aborted, codes.ResourceExhausted}
	for _, code := range transient {
		err := classify(status.Error(code, "unavailable"))
		require.ErrorIsf(t, err, usecase.ErrLedgerUnavailable, "code %s", code)
	}

	assert.NoError(t, classify(nil))
	assert.ErrorIs(t, classify(context.Canceled), usecase.ErrLedgerUnavailable)
}
