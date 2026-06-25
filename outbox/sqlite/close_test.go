package sqlite

import (
	"context"
	"testing"

	_ "modernc.org/sqlite"
)

// TestOutboxCloseDoesNotCloseSharedDB verifies the outbox stops its goroutine and
// releases its statements on Close but leaves the caller-owned *sql.DB open.
func TestOutboxCloseDoesNotCloseSharedDB(t *testing.T) {
	db := newTestDB(t)

	o, err := NewOutbox(db)
	if err != nil {
		t.Fatal(err)
	}
	o.Start()

	if err := o.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("shared DB must remain open after outbox.Close: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE probe_after_close (x INTEGER)`); err != nil {
		t.Fatalf("shared DB must remain writable after outbox.Close: %v", err)
	}
}
