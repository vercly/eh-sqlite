package eventing

import (
	"context"

	eh "github.com/vercly/eventhorizon"
)

// WithCorrelationIDMetadata returns an event option that adds the correlation ID from the context to the event metadata.
// If the correlation ID is not in the context, the option is a no-op.
func WithCorrelationIDMetadata(ctx context.Context) eh.EventOption {
	if id, ok := CorrelationIDFromContext(ctx); ok {
		return eh.WithMetadata(map[string]any{
			correlationIDKeyStr: id,
		})
	}
	return func(e eh.Event) {} // No-op event option
}

// CorrelationIDFromEvent extracts the correlation ID from an event's metadata.
func CorrelationIDFromEvent(e eh.Event) (string, bool) {
	if e.Metadata() == nil {
		return "", false
	}
	id, ok := e.Metadata()[correlationIDKeyStr].(string)
	return id, ok
}
