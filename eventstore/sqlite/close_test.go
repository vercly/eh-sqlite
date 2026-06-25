package sqlite

import (
	"context"
	"testing"

	_ "modernc.org/sqlite"
)

// TestCloseDoesNotCloseSharedDB verifies the event store releases its statements on
// Close but leaves the caller-owned *sql.DB open and usable.
func TestCloseDoesNotCloseSharedDB(t *testing.T) {
	db := newTestDB(t)

	store, err := NewEventStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("shared DB must remain open after store.Close: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE probe_after_close (x INTEGER)`); err != nil {
		t.Fatalf("shared DB must remain writable after store.Close: %v", err)
	}
}
