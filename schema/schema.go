// Package schema holds the shared migration bookkeeping and the stored
// timestamp contract used by every eh-sqlite component (outbox, durable
// commands, dead letters) and by offline operator tools.
//
// Bookkeeping lives in one namespaced table, eh_sqlite_migrations, keyed by
// (component, name). Components own their markers; PRAGMA user_version is
// never used because the database file is shared with other subsystems.
//
// Timestamp contract: every time.Time written by library code is passed as
// t.UTC(), so the mattn/go-sqlite3 driver stores the text form
// "YYYY-MM-DD HH:MM:SS.fffffffff+00:00". Text comparison in SQL is then
// monotone regardless of process time zone or DST. Historical rows written
// with a local offset are rewritten once by NormalizeColumnUTC.
package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// MigrationsTable is the shared bookkeeping table name.
const MigrationsTable = "eh_sqlite_migrations"

// StoredTimeLayout is the driver's first bind format; library code always
// binds t.UTC() so the offset suffix is "+00:00".
const StoredTimeLayout = "2006-01-02 15:04:05.999999999-07:00"

// ErrTimestampAmbiguous is returned (wrapped, with table/column/rowid) when a
// stored timestamp cannot be parsed. Migration fails closed on it.
var ErrTimestampAmbiguous = errors.New("eh-sqlite: stored timestamp cannot be normalized")

// storedLayouts lists every text layout the driver or SQLite itself may have
// produced. Layouts without an offset are UTC by SQLite contract
// (CURRENT_TIMESTAMP, datetime('now')).
var storedLayouts = []string{
	StoredTimeLayout,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999Z07:00",
	time.RFC3339Nano,
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// UTC converts t to UTC. A zero time stays zero.
func UTC(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	return t.UTC()
}

// ParseStored parses a timestamp text as stored by SQLite/mattn. Values without
// an offset are interpreted as UTC. Nanosecond precision is preserved.
func ParseStored(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("%w: empty value", ErrTimestampAmbiguous)
	}
	for _, layout := range storedLayouts {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("%w: %q", ErrTimestampAmbiguous, s)
}

// IsUTCText reports whether a stored text already carries the canonical
// "+00:00" (or "Z") offset. Offset-less text is UTC by contract but is still
// rewritten so that lexicographic comparison uses one uniform shape.
func IsUTCText(s string) bool {
	s = strings.TrimSpace(s)
	return strings.HasSuffix(s, "+00:00") || strings.HasSuffix(s, "Z")
}

// FormatStored renders t in the canonical stored text form (UTC).
func FormatStored(t time.Time) string {
	return t.UTC().Format(StoredTimeLayout)
}

// EnsureMigrationsTable creates the bookkeeping table if missing.
func EnsureMigrationsTable(db *sql.DB) error {
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			component  TEXT NOT NULL,
			name       TEXT NOT NULL,
			applied_at TIMESTAMP NOT NULL,
			PRIMARY KEY (component, name)
		)`, MigrationsTable)); err != nil {
		return fmt.Errorf("eh-sqlite: could not create migrations table: %w", err)
	}
	return nil
}

// IsApplied reports whether the (component, name) marker exists. A missing
// bookkeeping table means nothing was applied.
func IsApplied(ctx context.Context, db *sql.DB, component, name string) (bool, error) {
	exists, err := TableExists(ctx, db, MigrationsTable)
	if err != nil || !exists {
		return false, err
	}
	var n int
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE component = ? AND name = ?`, MigrationsTable),
		component, name).Scan(&n); err != nil {
		return false, fmt.Errorf("eh-sqlite: could not read migration marker %s/%s: %w", component, name, err)
	}
	return n > 0, nil
}

// Apply runs fn inside one transaction and records the marker in that same
// transaction. When the marker already exists nothing runs and applied is
// false. Any error from fn rolls everything back, marker included.
func Apply(ctx context.Context, db *sql.DB, component, name string, fn func(*sql.Tx) error) (applied bool, err error) {
	if err := EnsureMigrationsTable(db); err != nil {
		return false, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("eh-sqlite: could not begin migration %s/%s: %w", component, name, err)
	}
	// Write first (BEGIN IMMEDIATE semantics): the marker read below must not
	// start a read snapshot that a later write cannot upgrade under contention.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET name = name WHERE 0`, MigrationsTable)); err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("eh-sqlite: could not acquire write lock for migration %s/%s: %w", component, name, err)
	}
	defer func() {
		if rerr := tx.Rollback(); rerr != nil && !errors.Is(rerr, sql.ErrTxDone) {
			if err == nil {
				err = rerr
			}
		}
	}()

	var n int
	if err := tx.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE component = ? AND name = ?`, MigrationsTable),
		component, name).Scan(&n); err != nil {
		return false, fmt.Errorf("eh-sqlite: could not read migration marker %s/%s: %w", component, name, err)
	}
	if n > 0 {
		return false, nil
	}
	if fn != nil {
		if err := fn(tx); err != nil {
			return false, fmt.Errorf("eh-sqlite: migration %s/%s failed: %w", component, name, err)
		}
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (component, name, applied_at) VALUES (?, ?, ?)`, MigrationsTable),
		component, name, UTC(time.Now())); err != nil {
		return false, fmt.Errorf("eh-sqlite: could not record migration %s/%s: %w", component, name, err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("eh-sqlite: could not commit migration %s/%s: %w", component, name, err)
	}
	return true, nil
}

// NormalizeColumnUTC rewrites every non-NULL value of table.column whose text
// is not already in canonical UTC form. Offset-less values are UTC by SQLite
// contract. An unparseable value aborts with ErrTimestampAmbiguous (wrapped
// with table, column and rowid) and nothing is written by this call; the
// caller's transaction decides the rollback.
func NormalizeColumnUTC(ctx context.Context, tx *sql.Tx, table, column string) (rewritten int64, err error) {
	if err := ValidateIdent(table); err != nil {
		return 0, err
	}
	if err := ValidateIdent(column); err != nil {
		return 0, err
	}
	// Read as raw text: CAST avoids the driver parsing TIMESTAMP columns into time.Time.
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(
		`SELECT rowid, CAST(%[2]s AS TEXT) FROM %[1]s WHERE %[2]s IS NOT NULL`, table, column))
	if err != nil {
		return 0, fmt.Errorf("eh-sqlite: could not scan %s.%s: %w", table, column, err)
	}
	type fix struct {
		rowid int64
		value string
	}
	var fixes []fix
	for rows.Next() {
		var rowid int64
		var raw string
		if err := rows.Scan(&rowid, &raw); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("eh-sqlite: could not read %s.%s: %w", table, column, err)
		}
		parsed, perr := ParseStored(raw)
		if perr != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("%w (table %s, column %s, rowid %d)", perr, table, column, rowid)
		}
		canonical := FormatStored(parsed)
		if raw == canonical {
			continue // already byte-identical to what the driver writes for t.UTC()
		}
		fixes = append(fixes, fix{rowid: rowid, value: canonical})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, f := range fixes {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %[1]s SET %[2]s = ? WHERE rowid = ?`, table, column), f.value, f.rowid); err != nil {
			return rewritten, fmt.Errorf("eh-sqlite: could not rewrite %s.%s rowid %d: %w", table, column, f.rowid, err)
		}
		rewritten++
	}
	return rewritten, nil
}

// TableExists reports whether a user table or view exists.
func TableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	if err := ValidateIdent(table); err != nil {
		return false, err
	}
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view') AND name = ?`, table).Scan(&n); err != nil {
		return false, fmt.Errorf("eh-sqlite: could not probe table %s: %w", table, err)
	}
	return n > 0, nil
}

// TableExistsTx is TableExists inside a transaction.
func TableExistsTx(ctx context.Context, tx *sql.Tx, table string) (bool, error) {
	if err := ValidateIdent(table); err != nil {
		return false, err
	}
	var n int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view') AND name = ?`, table).Scan(&n); err != nil {
		return false, fmt.Errorf("eh-sqlite: could not probe table %s: %w", table, err)
	}
	return n > 0, nil
}

// ColumnExistsTx reports whether table has column, via PRAGMA table_info.
func ColumnExistsTx(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	if err := ValidateIdent(table); err != nil {
		return false, err
	}
	if err := ValidateIdent(column); err != nil {
		return false, err
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

// AddColumnIfAbsentTx runs ALTER TABLE ADD COLUMN only when the column is
// missing (probed through PRAGMA table_info, never by error text).
func AddColumnIfAbsentTx(ctx context.Context, tx *sql.Tx, table, column, decl string) error {
	if strings.TrimSpace(decl) == "" || strings.ContainsAny(decl, ";") {
		return fmt.Errorf("eh-sqlite: invalid column declaration for %s.%s", table, column)
	}
	has, err := ColumnExistsTx(ctx, tx, table, column)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, decl))
	return err
}

// ValidateIdent accepts plain SQL identifiers (letters, digits, underscore).
func ValidateIdent(name string) error {
	if name == "" {
		return errors.New("eh-sqlite: empty SQL identifier")
	}
	for i, r := range name {
		if r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r)) {
			continue
		}
		return fmt.Errorf("eh-sqlite: invalid SQL identifier %q", name)
	}
	return nil
}
