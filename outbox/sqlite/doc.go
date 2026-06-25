// Package sqlite implements the eventhorizon Outbox interface on top of
// database/sql and SQLite.
//
// The outbox operates on a *sql.DB supplied by the caller; it never opens or closes
// that database (Close stops the processing goroutine and releases the prepared
// statements only). The SQLite driver and DSN are the caller's choice.
//
// Rows are claimed atomically with a single UPDATE ... RETURNING, so multiple
// processes sharing the database can never dispatch the same event twice. A claimed
// row that is not deleted within PeriodicSweepAge becomes claimable again, which
// gives at-least-once delivery with crash recovery.
package sqlite
