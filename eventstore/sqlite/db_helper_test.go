package sqlite

import "strings"

// addTimestampParams appends modernc.org/sqlite DSN options that make the driver
// convert integer TIMESTAMP/DATETIME columns to time.Time automatically. It is a
// test-only helper: choosing a driver and its DSN is the responsibility of the
// library's consumer, not of the event store itself.
func addTimestampParams(dsn string) string {
	// Strip legacy mattn/go-sqlite3 parameters to avoid overlapping config
	dsn = strings.ReplaceAll(dsn, "_journal=wal", "")
	dsn = strings.ReplaceAll(dsn, "_journal=WAL", "")

	// Clean up any double ampersands or trailing query separators left from replacing
	dsn = strings.ReplaceAll(dsn, "&&", "&")
	dsn = strings.TrimSuffix(dsn, "&")
	dsn = strings.TrimSuffix(dsn, "?")

	sep := "&"
	if !strings.Contains(dsn, "?") {
		sep = "?"
	}
	return dsn + sep + "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)&_loc=auto&_inttotime=1"
}
