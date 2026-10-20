package usecase_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

func TestBuildPayoutCommandEnvelopeRefundMarker(t *testing.T) {
	t.Parallel()
	refundInvoiceID := "01a138fd-0000-7000-8000-000000000901"
	payout := storage.Payout{
		ID:                 "01a138fd-0000-7000-8000-000000000902",
		AmountMinor:        100_000,
		AmountAsset:        "USDT",
		AmountDecimals:     2,
		Network:            "trc20",
		DestinationAddress: "TXyzAddress0123456789",
		RefundInvoiceID:    &refundInvoiceID,
	}

	envelope, err := usecase.BuildPayoutCommandEnvelope(payout)
	require.NoError(t, err)
	var command eventsv1.PayoutCommand
	require.NoError(t, proto.Unmarshal(envelope.Payload, &command))
	assert.Equal(t, refundInvoiceID, command.GetRefundInvoiceId(),
		"a refund payout carries the source invoice in the command (17.12)")

	// A normal payout keeps the marker empty.
	payout.RefundInvoiceID = nil
	envelope, err = usecase.BuildPayoutCommandEnvelope(payout)
	require.NoError(t, err)
	require.NoError(t, proto.Unmarshal(envelope.Payload, &command))
	assert.Empty(t, command.GetRefundInvoiceId())
}
