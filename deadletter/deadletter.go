package deadletter

import (
	"context"
	"time"
)

// Record is the durable representation of a dead-lettered event or command.
type Record struct {
	ID                string    `json:"id"`
	Source            string    `json:"source"`
	EventType         string    `json:"event_type"`
	AggregateID       string    `json:"aggregate_id"`
	HandlerType       string    `json:"handler_type"`
	OutboxID          string    `json:"outbox_id,omitempty"`
	RemainingHandlers string    `json:"remaining_handlers"`
	Blob              string    `json:"blob"`
	Error             string    `json:"error"`
	RetryCount        int       `json:"retry_count"`
	CreatedAt         time.Time `json:"created_at"`
	DeadAt            time.Time `json:"dead_at"`
}

// Exporter is called after a dead-letter row has been inserted successfully.
// Export errors must not roll back the database write.
type Exporter interface {
	ExportDeadLetter(context.Context, Record) error
}

// ExportFunc adapts a function to Exporter.
type ExportFunc func(context.Context, Record) error

func (f ExportFunc) ExportDeadLetter(ctx context.Context, record Record) error {
	return f(ctx, record)
}
