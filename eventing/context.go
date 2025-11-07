package eventing

import (
	"context"
	"net/http"

	ehuuid "github.com/vercly/eventhorizon/uuid"
)

const (
	correlationIDKey    = "correlation_id"
	correlationIDKeyStr = "correlation_id"
)

// NewContextWithID returns a new context with a given correlation ID.
func NewContextWithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey, id)
}

// NewContextWithCorrelationID returns a new context with a correlation ID.
// It extracts the ID from the X-Correlation-ID header of the request.
// If the header is not present, a new UUID is generated.
func NewContextWithCorrelationID(ctx context.Context, r *http.Request) context.Context {
	id := r.Header.Get("X-Correlation-ID")
	if id == "" {
		id = ehuuid.New().String()
	}
	return context.WithValue(ctx, correlationIDKey, id)
}

// CorrelationIDFromContext returns the correlation ID from the context, if it exists.
func CorrelationIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationIDKey).(string)
	return id, ok
}

// RegisterContextMarshaler registers a marshaler for the correlation ID in the context.
func RegisterContextMarshaler(m func(context.Context, map[string]any) (map[string]any, error)) {
	// This function is a placeholder for a real implementation.
}

// RegisterContextUnmarshaler registers an unmarshaler for the correlation ID in the context.
func RegisterContextUnmarshaler(m func(context.Context, map[string]any) (context.Context, error)) {
	// This function is a placeholder for a real implementation.
}