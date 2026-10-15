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

// TestClassify разделяет бизнес-отказы Ledger и транспортную недоступность:
// первые завершают выплату компенсацией, вторые - повторяются.
func TestClassify(t *testing.T) {
	t.Parallel()

	business := []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.NotFound, codes.AlreadyExists}
	for _, code := range business {
		err := classify(status.Error(code, "rejected"))
		require.ErrorIsf(t, err, usecase.ErrLedgerRejected, "код %s", code)
		assert.Falsef(t, errors.Is(err, usecase.ErrLedgerUnavailable), "код %s", code)
	}
	transient := []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal, codes.Aborted, codes.ResourceExhausted}
	for _, code := range transient {
		err := classify(status.Error(code, "unavailable"))
		require.ErrorIsf(t, err, usecase.ErrLedgerUnavailable, "код %s", code)
	}

	assert.NoError(t, classify(nil))
	assert.ErrorIs(t, classify(context.Canceled), usecase.ErrLedgerUnavailable)
}
