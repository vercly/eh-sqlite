package deadletter

import (
	"database/sql"
	"fmt"
	"strings"
	"unicode"
)

// EnsureSchema creates the shared dead_letters table and indexes used by outbox
// and durable command handling.
//
// Existing databases are migrated in place: exported_at / replayed_* are added
// if missing (via PRAGMA table_info, never by matching ALTER error text), any
// historical duplicates of (source, outbox_id, handler_type) with a non-NULL
// outbox_id are collapsed, then a unique index is created for idempotent
// terminal finalization. Rows with NULL outbox_id (no-match, commands) stay
// unconstrained by that unique key — SQLite treats each NULL as distinct.
//
// Order matters for old schemas: CREATE TABLE IF NOT EXISTS is a no-op when the
// table already exists without newer columns, so column migration runs before
// any index that references those columns.
func EnsureSchema(db *sql.DB, tableName string) error {
	if err := validateIdent(tableName); err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
			id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			handler_type TEXT NOT NULL,
			outbox_id TEXT,
			remaining_handlers TEXT,
			blob TEXT NOT NULL,
			error TEXT NOT NULL,
			retry_count INTEGER DEFAULT 0,
			created_at TIMESTAMP NOT NULL,
			dead_at TIMESTAMP NOT NULL,
			exported_at TIMESTAMP,
			replayed_at TIMESTAMP,
			replayed_by TEXT
		);
	`, tableName)); err != nil {
		return fmt.Errorf("could not create dead letters schema: %w", err)
	}

	// Additive columns for older deployments. Probe with PRAGMA table_info —
	// never classify "duplicate column" by error string (AGENTS).
	for _, col := range []struct {
		name string
		decl string
	}{
		{"exported_at", "TIMESTAMP"},
		{"replayed_at", "TIMESTAMP"},
		{"replayed_by", "TEXT"},
	} {
		if err := AddColumnIfAbsent(db, tableName, col.name, col.decl); err != nil {
			return fmt.Errorf("could not migrate dead letters column %s: %w", col.name, err)
		}
	}

	if _, err := db.Exec(fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_%[1]s_source_created ON %[1]s (source, created_at);
		CREATE INDEX IF NOT EXISTS idx_%[1]s_exported_dead_at ON %[1]s (exported_at, dead_at);
	`, tableName)); err != nil {
		return fmt.Errorf("could not create dead letters indexes: %w", err)
	}

	// Collapse crash-induced duplicates before CREATE UNIQUE INDEX. Prefer a row
	// that was already exported (exported_at IS NOT NULL) so cleanup retention
	// markers survive; among ties use the lowest rowid for stability.
	if _, err := db.Exec(fmt.Sprintf(`
		DELETE FROM %[1]s
		WHERE rowid IN (
			SELECT rowid FROM (
				SELECT rowid,
					ROW_NUMBER() OVER (
						PARTITION BY source, outbox_id, handler_type
						ORDER BY (exported_at IS NULL), rowid
					) AS rn
				FROM %[1]s
				WHERE outbox_id IS NOT NULL
			)
			WHERE rn > 1
		)
	`, tableName)); err != nil {
		return fmt.Errorf("could not dedupe dead letters for unique index: %w", err)
	}

	if _, err := db.Exec(fmt.Sprintf(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_%[1]s_source_outbox_handler
		ON %[1]s (source, outbox_id, handler_type)
	`, tableName)); err != nil {
		return fmt.Errorf("could not create dead letters uniqueness index: %w", err)
	}
	return nil
}

// TableExists reports whether a user table/view with the given name exists.
func TableExists(db *sql.DB, tableName string) (bool, error) {
	if err := validateIdent(tableName); err != nil {
		return false, err
	}
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view') AND name = ?`,
		tableName,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ColumnExists reports whether tableName has a column named col via PRAGMA table_info.
func ColumnExists(db *sql.DB, tableName, col string) (bool, error) {
	if err := validateIdent(tableName); err != nil {
		return false, err
	}
	if err := validateIdent(col); err != nil {
		return false, err
	}
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notnull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}

// AddColumnIfAbsent runs ALTER TABLE ADD COLUMN only when PRAGMA table_info
// shows the column is missing. Avoids duplicate-column error string matching.
func AddColumnIfAbsent(db *sql.DB, tableName, col, decl string) error {
	if err := validateIdent(tableName); err != nil {
		return err
	}
	if err := validateIdent(col); err != nil {
		return err
	}
	// decl is a type/default fragment controlled by callers (e.g. "TIMESTAMP").
	if strings.TrimSpace(decl) == "" || strings.ContainsAny(decl, ";") {
		return fmt.Errorf("invalid column declaration")
	}
	has, err := ColumnExists(db, tableName, col)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	_, err = db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, tableName, col, decl))
	return err
}

func validateIdent(name string) error {
	if name == "" {
		return fmt.Errorf("empty SQL identifier")
	}
	for i, r := range name {
		if r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r)) {
			continue
		}
		return fmt.Errorf("invalid SQL identifier %q", name)
	}
	return nil
}
