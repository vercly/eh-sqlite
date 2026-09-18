package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vercly/eh-sqlite/schema"
)

// Table name suffixes derived from the WithTableName prefix (default "outbox").
const (
	suffixPublications = "_publications"
	suffixDeliveries   = "_deliveries"
	suffixV1Migrated   = "_v1_migrated"
)

// Migration bookkeeping identifiers (schema.Apply component/name).
const (
	migrationComponent = "outbox"
	migrationV2Name    = "v2_deliveries"
)

// Publication origins (bounded set).
const (
	originPublish  = "publish"
	originMigrated = "migrated"
	originReplay   = "replay"
)

func publicationsTableFor(prefix string) string { return prefix + suffixPublications }
func deliveriesTableFor(prefix string) string   { return prefix + suffixDeliveries }
func v1MigratedTableFor(prefix string) string   { return prefix + suffixV1Migrated }

// migrationNameFor keeps one marker per table prefix so a custom prefix does
// not share bookkeeping with the default one.
func migrationNameFor(prefix string) string {
	if prefix == "outbox" {
		return migrationV2Name
	}
	return migrationV2Name + ":" + prefix
}

// createV2Schema creates the publication and delivery tables plus indexes.
// Idempotent. Runs inside the caller's transaction.
func createV2Schema(ctx context.Context, tx *sql.Tx, prefix string) error {
	if err := schema.ValidateIdent(prefix); err != nil {
		return err
	}
	pubs := publicationsTableFor(prefix)
	dels := deliveriesTableFor(prefix)
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			publication_id TEXT PRIMARY KEY,
			event_type     TEXT NOT NULL,
			aggregate_id   TEXT NOT NULL,
			partition_key  TEXT NOT NULL DEFAULT '',
			event_blob     TEXT NOT NULL,
			created_at     TIMESTAMP NOT NULL,
			origin         TEXT NOT NULL DEFAULT 'publish',
			origin_ref     TEXT
		)`, pubs),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			seq              INTEGER PRIMARY KEY AUTOINCREMENT,
			id               TEXT NOT NULL UNIQUE,
			publication_id   TEXT NOT NULL REFERENCES %s(publication_id),
			handler_type     TEXT,
			dispatch_key     TEXT,
			dispatch_config  TEXT,
			event_type       TEXT NOT NULL,
			aggregate_id     TEXT NOT NULL,
			created_at       TIMESTAMP NOT NULL,
			available_at     TIMESTAMP NOT NULL,
			taken_at         TIMESTAMP,
			retry_count      INTEGER NOT NULL DEFAULT 0,
			unresolved_at    TIMESTAMP,
			legacy_outbox_id TEXT
		)`, dels, pubs),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_claim ON %[1]s (dispatch_key, taken_at, available_at, seq)`, dels),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_publication ON %[1]s (publication_id)`, dels),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_taken_available ON %[1]s (taken_at, available_at)`, dels),
		// Timer lookup: first row with available_at > now, no scan of due rows.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_available ON %[1]s (available_at, seq)`, dels),
		// Rematch sentinels are rare; a partial index keeps their probe O(log n).
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_sentinel ON %[1]s (available_at, seq) WHERE handler_type IS NULL`, dels),
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("could not create outbox v2 schema: %w", err)
		}
	}
	return nil
}
