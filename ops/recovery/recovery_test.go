package recovery

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestValidateRemapOptionsDryRunDoesNotRequireApplyConfirmations(t *testing.T) {
	opts, err := ValidateRemapOptions(RemapOptions{DeliveryIDs: []string{" d1 ", "d1", ""}, Mapping: map[string]string{"old": "new"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.DeliveryIDs) != 1 || opts.DeliveryIDs[0] != "d1" {
		t.Fatalf("ids = %#v", opts.DeliveryIDs)
	}
}

func TestValidateRemapOptionsApplySafetyRequirements(t *testing.T) {
	base := RemapOptions{
		Apply: true, Actor: "ops", DeliveryIDs: []string{"delivery-1"},
		Mapping:          map[string]string{"legacy": "stable"},
		ProcessorStopped: true, BackupConfirmed: true,
	}
	if _, err := ValidateRemapOptions(base); err != nil {
		t.Fatalf("valid options: %v", err)
	}
	for name, mutate := range map[string]func(*RemapOptions){
		"actor":     func(o *RemapOptions) { o.Actor = "" },
		"ids":       func(o *RemapOptions) { o.DeliveryIDs = nil },
		"mapping":   func(o *RemapOptions) { o.Mapping = nil },
		"processor": func(o *RemapOptions) { o.ProcessorStopped = false },
		"backup":    func(o *RemapOptions) { o.BackupConfirmed = false },
	} {
		t.Run(name, func(t *testing.T) {
			o := base
			mutate(&o)
			if _, err := ValidateRemapOptions(o); err == nil {
				t.Fatal("want validation error")
			}
		})
	}
}

func TestValidateRemapOptionsRejectsUnsafeMappings(t *testing.T) {
	base := RemapOptions{Apply: true, Actor: "ops", DeliveryIDs: []string{"delivery-1"}, ProcessorStopped: true, BackupConfirmed: true}
	for name, mapping := range map[string]map[string]string{
		"empty old": {"": "stable"},
		"empty new": {"legacy": ""},
		"same":      {"legacy": "legacy"},
	} {
		t.Run(name, func(t *testing.T) {
			base.Mapping = mapping
			if _, err := ValidateRemapOptions(base); err == nil {
				t.Fatal("want validation error")
			}
		})
	}
}

func TestListUnclaimedAndDryRunDoNotMutateSchema(t *testing.T) {
	db := openV2DB(t)
	seedDelivery(t, db, "d-unclaimed", "p-1", "legacy-handler", true)
	seedDelivery(t, db, "d-live", "p-1", "current-handler", false)

	before := schemaVersion(t, db)
	rows, err := ListUnclaimed(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "d-unclaimed" || rows[0].HandlerType == nil || *rows[0].HandlerType != "legacy-handler" {
		t.Fatalf("unclaimed = %#v", rows)
	}
	res, err := RemapRecipients(context.Background(), db, RemapOptions{
		DeliveryIDs: []string{"d-unclaimed"}, Mapping: map[string]string{"legacy-handler": "stable-handler"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.DryRun || res.Errors != 0 || len(res.Items) != 1 || res.Items[0].Status != "would_remap" {
		t.Fatalf("result = %+v", res)
	}
	if schemaVersion(t, db) != before || tableExists(t, db, "outbox_recipient_remap_audit") {
		t.Fatal("dry-run mutated schema")
	}
	var handler string
	var unresolved sql.NullTime
	if err := db.QueryRow(`SELECT handler_type, unresolved_at FROM outbox_deliveries WHERE id = 'd-unclaimed'`).Scan(&handler, &unresolved); err != nil {
		t.Fatal(err)
	}
	if handler != "legacy-handler" || !unresolved.Valid {
		t.Fatalf("dry-run mutated delivery: handler=%q unresolved=%v", handler, unresolved)
	}
}

func TestRemapRecipientsAppliesSelectedRowsAndWritesAudit(t *testing.T) {
	db := openV2DB(t)
	seedDelivery(t, db, "d-1", "p-1", "legacy-a", true)
	seedDelivery(t, db, "d-2", "p-1", "legacy-b", true)
	res, err := RemapRecipients(context.Background(), db, RemapOptions{
		Apply: true, Actor: "operator@example", DeliveryIDs: []string{"d-1"},
		Mapping: map[string]string{"legacy-a": "stable-a"}, ProcessorStopped: true, BackupConfirmed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Applied != 1 || res.Errors != 0 || res.Items[0].Status != "remapped" {
		t.Fatalf("result = %+v", res)
	}
	var handler string
	var unresolved, key, config sql.NullString
	if err := db.QueryRow(`SELECT handler_type, unresolved_at, dispatch_key, dispatch_config FROM outbox_deliveries WHERE id = 'd-1'`).Scan(&handler, &unresolved, &key, &config); err != nil {
		t.Fatal(err)
	}
	if handler != "stable-a" || unresolved.Valid || key.Valid || config.Valid {
		t.Fatalf("delivery was not reset for startup recomputation: %q %#v %#v %#v", handler, unresolved, key, config)
	}
	if err := db.QueryRow(`SELECT old_recipient, new_recipient, remapped_by FROM outbox_recipient_remap_audit WHERE delivery_id = 'd-1'`).Scan(&handler, &key, &config); err != nil {
		t.Fatal(err)
	}
	if handler != "legacy-a" || key.String != "stable-a" || config.String != "operator@example" {
		t.Fatalf("bad audit: %q %#v %#v", handler, key, config)
	}
	if err := db.QueryRow(`SELECT handler_type FROM outbox_deliveries WHERE id = 'd-2'`).Scan(&handler); err != nil || handler != "legacy-b" {
		t.Fatalf("unselected delivery changed: %q %v", handler, err)
	}
}

func TestRemapRecipientsRejectsMissingMappingAndSentinelWithoutWrites(t *testing.T) {
	db := openV2DB(t)
	seedDelivery(t, db, "d-unknown", "p-1", "legacy", true)
	seedSentinel(t, db, "d-sentinel", "p-1")
	for _, tc := range []struct {
		name    string
		id      string
		mapping map[string]string
	}{
		{"missing mapping", "d-unknown", map[string]string{"other": "stable"}},
		{"sentinel", "d-sentinel", map[string]string{"legacy": "stable"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := RemapRecipients(context.Background(), db, RemapOptions{
				Apply: true, Actor: "ops", DeliveryIDs: []string{tc.id}, Mapping: tc.mapping,
				ProcessorStopped: true, BackupConfirmed: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if res.Errors != 1 || tableExists(t, db, "outbox_recipient_remap_audit") {
				t.Fatalf("unsafe remap mutated db: %+v", res)
			}
		})
	}
}

func openV2DB(t *testing.T) *sql.DB {
	t.Helper()
	f, err := os.CreateTemp("", "recovery-*.db")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(); _ = os.Remove(f.Name()) })
	if _, err := db.Exec(`
		CREATE TABLE eh_sqlite_migrations (component TEXT NOT NULL, name TEXT NOT NULL, applied_at TIMESTAMP NOT NULL, PRIMARY KEY (component, name));
		INSERT INTO eh_sqlite_migrations VALUES ('outbox', 'v2_deliveries', '2026-01-01 00:00:00+00:00');
		CREATE TABLE outbox_publications (publication_id TEXT PRIMARY KEY);
		CREATE TABLE outbox_deliveries (
			seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, publication_id TEXT NOT NULL,
			handler_type TEXT, dispatch_key TEXT, dispatch_config TEXT, event_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL, created_at TIMESTAMP NOT NULL, available_at TIMESTAMP NOT NULL,
			taken_at TIMESTAMP, retry_count INTEGER NOT NULL DEFAULT 0, unresolved_at TIMESTAMP, legacy_outbox_id TEXT
		);`); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedDelivery(t *testing.T, db *sql.DB, id, publication, handler string, unresolved bool) {
	t.Helper()
	now := time.Now().UTC()
	var unresolvedAt any
	if unresolved {
		unresolvedAt = now
	}
	if _, err := db.Exec(`INSERT OR IGNORE INTO outbox_publications (publication_id) VALUES (?)`, publication); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO outbox_deliveries (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id, created_at, available_at, unresolved_at) VALUES (?, ?, ?, 'old-key', 'old-config', 'event', 'aggregate', ?, ?, ?)`, id, publication, handler, now, now, unresolvedAt); err != nil {
		t.Fatal(err)
	}
}

func seedSentinel(t *testing.T, db *sql.DB, id, publication string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := db.Exec(`INSERT INTO outbox_deliveries (id, publication_id, event_type, aggregate_id, created_at, available_at, unresolved_at) VALUES (?, ?, 'event', 'aggregate', ?, ?, ?)`, id, publication, now, now, now); err != nil {
		t.Fatal(err)
	}
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func schemaVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`PRAGMA schema_version`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
