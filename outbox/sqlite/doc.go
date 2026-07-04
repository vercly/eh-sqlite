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
// AddHandler registers handlers with Serial dispatch by default. Use
// AddHandlerWithOptions with WithDispatchMode(PartitionByAggregate) and
// WithPartitionShards to opt into per-aggregate ordered shards.
//
// Terminal dead-lettering is per handler and at-least-once: a crash after the
// dead_letters insert but before the handlers list update can produce a
// duplicate dead letter, but the handler is not silently dropped. The file
// exporter is called only for terminal outbox and command dead letters after
// they are written to the DB. No-match dead letters created during
// in-transaction publish are not exported before commit, so rolled-back writes
// do not leak to the filesystem.
//
// When HandleEvent is called with an external transaction in the context via
// context/sqlite.NewContextWithTx, the caller must call NotifyAfterCommit after
// committing that transaction. eventstore/sqlite.EventStore.Save does this
// automatically for in-transaction handlers that implement NotifyAfterCommit.
package sqlite
