package deadletter

import (
	"database/sql"
	"fmt"
)

// EnsureSchema creates the shared dead_letters table and indexes used by outbox
// and durable command handling.
func EnsureSchema(db *sql.DB, tableName string) error {
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
			dead_at TIMESTAMP NOT NULL
		);

		CREATE INDEX IF NOT EXISTS idx_%[1]s_source_created ON %[1]s (source, created_at);
	`, tableName)); err != nil {
		return fmt.Errorf("could not create dead letters schema: %w", err)
	}
	return nil
}
