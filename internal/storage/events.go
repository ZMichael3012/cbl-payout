package storage

import (
	"context"
	"regexp"
	"time"

	"gorm.io/gorm"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
)

// TransitionEvent describes an applied state transition (including creation
// with an empty from) for publication to payout.events (10.2): a payout
// snapshot after the transition and journal metadata.
type TransitionEvent struct {
	// Payout sets the payout snapshot after the transition (including the new version).
	Payout Payout
	// From sets the state before the transition (empty for creation).
	From domain.State
	// To sets the state after the transition.
	To domain.State
	// OccurredAt sets the transition time (the journal row).
	OccurredAt time.Time
	// Traceparent sets the W3C trace context for the envelope (if known).
	Traceparent string
}

// TransitionEventSink writes a payout.events event to the outbox in the
// transition transaction: the event and the business change are committed or
// rolled back together.
type TransitionEventSink interface {
	// AppendTransitionEvent appends the event envelope in the passed transaction.
	AppendTransitionEvent(ctx context.Context, tx *gorm.DB, event TransitionEvent) error
}

// WithEventSink enables payout.events writing; without a sink no events are written.
func WithEventSink(sink TransitionEventSink) Option {
	return func(r *Repository) { r.eventSink = sink }
}

// AppendOutbox appends an envelope to the outbox in the passed transaction:
// saga 10.2 writes the payout.commands command atomically with transitions.
func (r *Repository) AppendOutbox(ctx context.Context, tx *gorm.DB, envelope outbox.Envelope) error {
	return outbox.NewStore().Append(ctx, tx, envelope)
}

// emitTransitionEvent passes the event to the sink if it is configured; an
// explicitly set traceparent is not overwritten by the context value.
func (r *Repository) emitTransitionEvent(ctx context.Context, tx *gorm.DB, event TransitionEvent) error {
	if r.eventSink == nil {
		return nil
	}
	if event.Traceparent == "" {
		event.Traceparent = traceparentFromContext(ctx)
	}
	return r.eventSink.AppendTransitionEvent(ctx, tx, event)
}

// traceparentKey sets the context key for the W3C traceparent.
type traceparentKey struct{}

// traceparentPattern sets the W3C trace context format; an arbitrary string
// from a Kafka header must not reach envelopes and downstream logs.
var traceparentPattern = regexp.MustCompile(`^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

// WithTraceparent stores the traceparent in the context: the consumer puts
// the gateway event header there, and payout events inherit the source trace.
// A header that does not match the W3C format is ignored.
func WithTraceparent(ctx context.Context, traceparent string) context.Context {
	if !traceparentPattern.MatchString(traceparent) {
		return ctx
	}
	return context.WithValue(ctx, traceparentKey{}, traceparent)
}

// traceparentFromContext extracts the traceparent from the context.
func traceparentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceparentKey{}).(string)
	return value
}
