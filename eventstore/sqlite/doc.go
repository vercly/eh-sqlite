// Package sqlite implements the eventhorizon EventStore and SnapshotStore
// interfaces on top of database/sql and SQLite.
//
// The store operates on a *sql.DB supplied by the caller; it never opens or closes
// that database (Close releases only the prepared statements). The SQLite driver
// and DSN are therefore the caller's choice — the package is driver-agnostic and
// does not import a driver.
//
// Global ordering: every saved event receives a contiguous global position,
// allocated atomically from the $all stream under SQLite's single-writer lock, so
// position order equals commit order even across connections or processes.
// Per-aggregate optimistic concurrency is enforced via the stream version check,
// returning eh.ErrEventConflictFromOtherSave on conflict.
package sqlite
