// Package sqlite implements an Event Horizon outbox backed by SQLite.
//
// Lifecycle (one process per DB file; stop-first operational exclusivity):
//
//  1. AddHandler / AddHandlerWithOptions while registration is open.
//  2. StartChecked (preferred) or Start closes registration, resets taken_at in
//     one transaction, then starts the fetcher only after a successful reset.
//     On reset failure the fetcher does not start; registration stays closed and
//     StartChecked may retry. eh.Outbox.Start is a fail-closed wrapper that
//     reports reset errors on Errors() without a return value.
//  3. HandleEvent publishes only when the processor is running (after a
//     successful reset). Fail-closed Start leaves registration closed but
//     publish still returns ErrOutboxNotStarted until a successful retry.
//  4. AddHandler after Start/StartChecked fails with ErrOutboxAlreadyStarted.
//  5. A successful Start/StartChecked is idempotent; further calls are no-ops.
//
// Claim remains per outbox row via taken_at (no lease renewal, no deliveries
// table). Before claim, the process reserves capacity on every long-lived
// dispatch-key queue for the record and a coordinator slot (WithAdmissionLimit).
// If any queue is full the whole record is skipped (no taken_at, no blocking
// send). Fetch is decoupled from completion: the fetcher does not wait for
// handlers before claiming more records. Per-key FIFO workers run deliveries;
// WithMaxGoroutines is a global HandleEvent semaphore (fairness per key worker).
// WithQueueDepth bounds each key queue (default 32; values < 1 are rejected).
// WithDispatchStats observes queue depth and HandleEvent in-flight with labels
// handler+shard only (serial uses shard "none").
// Admission is not durable: after crash + Start reset, rows may be redelivered
// (at-least-once). Selection order is available_at, created_at, id.
//
// Delayed dispatch can be requested with event metadata key "outbox.available_at"
// or with WithAvailableAt/WithDelay on the context. Metadata takes precedence
// over the context helper; if neither is present the event is available now.
//
// AddHandler registers handlers with Serial dispatch by default. Use
// AddHandlerWithOptions with WithDispatchMode(PartitionByAggregate) and
// WithPartitionShards to opt into per-aggregate ordered shards.
//
// Terminal finalization is atomic per outbox record: dead_letters inserts,
// handlers list updates (or row delete), and retry fields (retry_count,
// available_at, taken_at) commit in one SQL transaction. Uniqueness of
// (source, outbox_id, handler_type) makes re-processing after a crash idempotent
// instead of duplicating DLQ rows. File export runs only after that transaction
// commits; export failures do not roll back the DB write. No-match dead letters
// created during in-transaction publish share the dead_letters table but are
// written on the publish path (not the processor finalize path).
//
// Rematch (handlers=[] after no_match DLQ replay): claim re-matches against the
// current registration, then persists the full matched handler list in the same
// claim transaction before dispatch. Finalize remaining work is computed from
// that list; leaving it empty would silently delete the row on any outcome.
//
// When HandleEvent is called with an external transaction in the context via
// context/sqlite.NewContextWithTx, the caller must call NotifyAfterCommit after
// committing that transaction. eventstore/sqlite.EventStore.Save does this
// automatically for in-transaction handlers that implement NotifyAfterCommit.
package sqlite
