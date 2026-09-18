package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	eh "github.com/vercly/eventhorizon"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/mocks"
	"github.com/vercly/eventhorizon/uuid"
)

func TestReviewContract_TableNameMigrationsAreIndependent(t *testing.T) {
	db := reviewContractDB(t)
	ctx := context.Background()

	first, err := NewOutbox(db, WithTableName("review_alpha"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, reviewContractHandler{typ: "review_alpha_handler"}); err != nil {
		t.Fatal(err)
	}
	if err := first.StartChecked(); err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	second, err := NewOutbox(db, WithTableName("review_beta"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, reviewContractHandler{typ: "review_beta_handler"}); err != nil {
		t.Fatal(err)
	}
	if err := second.StartChecked(); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		o      *Outbox
		prefix string
	}{{first, "review_alpha"}, {second, "review_beta"}} {
		if err := item.o.HandleEvent(WithAvailableAt(ctx, time.Now().Add(time.Hour)), reviewContractEvent(t)); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s_publications", item.prefix)).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("publication count for %s = %d, want 1", item.prefix, count)
		}
	}

	for _, table := range []string{"review_alpha_publications", "review_alpha_deliveries", "review_beta_publications", "review_beta_deliveries"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("table %s exists = %d, want 1", table, count)
		}
	}
}

func TestReviewContract_StartRecomputesDispatchAndClearsRematch(t *testing.T) {
	db := reviewContractDB(t)
	reviewMigrateV1(t, db, "review_contract", "review_handler")

	publication := uuid.New().String()
	created := "2026-01-01 00:00:00+00:00"
	future := "2030-01-01 00:00:00+00:00"
	if _, err := db.Exec(`INSERT INTO review_contract_publications
		(publication_id,event_type,aggregate_id,partition_key,event_blob,created_at,origin)
		VALUES (?,?,?,?,?,?,?)`, publication, mocks.EventType.String(), uuid.New().String(), "partition-key", reviewContractBlob(t), created, "publish"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO review_contract_deliveries
		(id,publication_id,handler_type,dispatch_key,dispatch_config,event_type,aggregate_id,created_at,available_at,taken_at,unresolved_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`, uuid.New().String(), publication, "review_handler", "review_handler:old", "mode=partition;shards=4", mocks.EventType.String(), uuid.New().String(), created, future, created, created); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO review_contract_deliveries
		(id,publication_id,handler_type,dispatch_key,dispatch_config,event_type,aggregate_id,created_at,available_at,unresolved_at)
		VALUES (?,?,?,?,?,?,?,?,?,?)`, uuid.New().String(), publication, nil, nil, nil, mocks.EventType.String(), uuid.New().String(), created, future, created); err != nil {
		t.Fatal(err)
	}

	o, err := NewOutbox(db, WithTableName("review_contract"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if err := o.AddHandlerWithOptions(context.Background(), eh.MatchEvents{mocks.EventType}, reviewContractHandler{typ: "review_handler"}, WithDispatchMode(PartitionByAggregate), WithPartitionShards(8)); err != nil {
		t.Fatal(err)
	}
	if err := o.StartChecked(); err != nil {
		t.Fatal(err)
	}

	var key, config string
	var taken, unresolved sql.NullString
	if err := db.QueryRow(`SELECT dispatch_key,dispatch_config,taken_at,unresolved_at FROM review_contract_deliveries WHERE handler_type='review_handler'`).Scan(&key, &config, &taken, &unresolved); err != nil {
		t.Fatal(err)
	}
	if key == "review_handler:old" || config != "mode=partition;shards=8" || taken.Valid || unresolved.Valid {
		t.Fatalf("startup recompute = key %q config %q taken=%v unresolved=%v", key, config, taken.Valid, unresolved.Valid)
	}
	var sentinelUnresolved sql.NullString
	if err := db.QueryRow(`SELECT unresolved_at FROM review_contract_deliveries WHERE handler_type IS NULL`).Scan(&sentinelUnresolved); err != nil {
		t.Fatal(err)
	}
	if sentinelUnresolved.Valid {
		t.Fatal("startup must clear unresolved_at on rematch sentinels")
	}
}

func TestReviewContract_MigrationCanonicalizesOffsetsAndOrder(t *testing.T) {
	db := reviewContractDB(t)
	if _, err := db.Exec(`CREATE TABLE review_tz (
		id TEXT PRIMARY KEY,event_type TEXT NOT NULL,aggregate_id TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL,available_at TIMESTAMP,taken_at TIMESTAMP,
		handlers TEXT NOT NULL,event_blob TEXT NOT NULL,retry_count INTEGER DEFAULT 0)`); err != nil {
		t.Fatal(err)
	}
	values := []string{
		"2026-01-01T05:00:00Z",
		"2026-01-01 05:00:00.500000000+00:00",
		"2026-01-01 06:00:00+01:00",
	}
	for _, available := range values {
		if _, err := db.Exec(`INSERT INTO review_tz
			(id,event_type,aggregate_id,created_at,available_at,handlers,event_blob)
			VALUES (?,?,?,?,?,?,?)`, uuid.New().String(), mocks.EventType.String(), uuid.New().String(), available, available, `["review_tz_handler"]`, reviewContractBlob(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Migrate(context.Background(), db, "review_tz"); err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query(`SELECT CAST(available_at AS TEXT) FROM review_tz_deliveries ORDER BY available_at,seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"2026-01-01 05:00:00+00:00", "2026-01-01 05:00:00+00:00", "2026-01-01 05:00:00.5+00:00"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("canonical available_at order = %v, want %v", got, want)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatalf("canonical timestamps are not lexically monotone: %v", got)
	}
}

type reviewContractHandler struct{ typ eh.EventHandlerType }

func (h reviewContractHandler) HandlerType() eh.EventHandlerType            { return h.typ }
func (h reviewContractHandler) HandleEvent(context.Context, eh.Event) error { return nil }

func reviewContractDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "review.db")+"?_txlock=immediate&_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL&_fk=1&_loc=auto")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func reviewContractEvent(t testing.TB) eh.Event {
	t.Helper()
	return eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "review-contract"}, time.Now().UTC(),
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
}

func reviewContractBlob(t testing.TB) string {
	t.Helper()
	blob, err := (&ehcodec.EventCodec{}).MarshalEvent(context.Background(), reviewContractEvent(t))
	if err != nil {
		t.Fatal(err)
	}
	return string(blob)
}

func reviewMigrateV1(t testing.TB, db *sql.DB, prefix, handler string) {
	t.Helper()
	if _, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id TEXT PRIMARY KEY,event_type TEXT NOT NULL,aggregate_id TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL,available_at TIMESTAMP,taken_at TIMESTAMP,
		handlers TEXT NOT NULL,event_blob TEXT NOT NULL,retry_count INTEGER DEFAULT 0)`, prefix)); err != nil {
		t.Fatal(err)
	}
	value := "2026-01-01 00:00:00+00:00"
	if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s
		(id,event_type,aggregate_id,created_at,available_at,handlers,event_blob)
		VALUES (?,?,?,?,?,?,?)`, prefix), uuid.New().String(), mocks.EventType.String(), uuid.New().String(), value, value, fmt.Sprintf(`["%s"]`, handler), reviewContractBlob(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(context.Background(), db, prefix); err != nil {
		t.Fatal(err)
	}
}
