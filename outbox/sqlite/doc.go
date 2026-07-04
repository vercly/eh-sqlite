// Package sqlite implements an Event Horizon outbox backed by SQLite.
//
// Call Start before publishing events with HandleEvent. Publishing before Start
// fails with ErrOutboxNotStarted so misconfigured transports fail fast instead
// of silently dropping messages.
//
// Delayed dispatch can be requested with event metadata key "outbox.available_at"
// or with WithAvailableAt/WithDelay on the context. Metadata takes precedence
// over the context helper; if neither is present the event is available now.
//
// When HandleEvent is called with an external transaction in the context via
// context/sqlite.NewContextWithTx, the caller must call NotifyAfterCommit after
// committing that transaction. eventstore/sqlite.EventStore.Save does this
// automatically for in-transaction handlers that implement NotifyAfterCommit.
package sqlite
