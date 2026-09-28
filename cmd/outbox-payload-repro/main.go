package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	outbox "github.com/vercly/eh-sqlite/outbox/sqlite"
	eh "github.com/vercly/eventhorizon"
)

var calls atomic.Int64

type handler string

func (h handler) HandlerType() eh.EventHandlerType            { return eh.EventHandlerType(h) }
func (h handler) HandleEvent(context.Context, eh.Event) error { calls.Add(1); return nil }
func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	dbPath := flag.String("db", "/data/repro.db", "synthetic SQLite database (seed requires empty DB)")
	mode := flag.String("mode", "run", "seed or run")
	size := flag.Int("bytes", 155993456, "base64 metadata length")
	admission := flag.Int("admission", 50, "admitted deliveries")
	workers := flag.Int("workers", 10, "concurrent handlers")
	flag.Parse()
	db, err := sql.Open("sqlite3", *dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	must(err)
	defer db.Close()
	o, err := outbox.NewOutbox(db, outbox.WithAdmissionLimit(*admission), outbox.WithMaxGoroutines(*workers))
	must(err)
	for i := 0; i < 51; i++ {
		must(o.AddHandler(context.Background(), eh.MatchAll{}, handler(fmt.Sprintf("synthetic-%02d", i))))
	}
	if *mode == "seed" {
		// Valid base64, entirely synthetic. No source database or external service.
		blob, err := json.Marshal(map[string]any{"event_type": "synthetic.Large", "timestamp": "2026-09-25T12:09:16Z", "aggregate_type": "synthetic", "aggregate_id": "00000000-0000-0000-0000-000000000001", "version": 1, "metadata": map[string]any{"legacy.body_base64": strings.Repeat("A", *size), "legacy.type": "synthetic.Large", "outbox.partition_key": "synthetic"}, "context": map[string]any{}})
		must(err)
		tx, err := db.Begin()
		must(err)
		_, err = tx.Exec("INSERT INTO outbox_publications(publication_id,event_type,aggregate_id,partition_key,event_blob,created_at) VALUES(?,?,?,?,?,?)", "synthetic", "synthetic.Large", "00000000-0000-0000-0000-000000000001", "synthetic", string(blob), "2026-09-25 12:09:16+00:00")
		must(err)
		for i := 0; i < 51; i++ {
			_, err = tx.Exec("INSERT INTO outbox_deliveries(id,publication_id,handler_type,event_type,aggregate_id,created_at,available_at) VALUES(?,?,?,?,?,?,?)", fmt.Sprintf("d-%02d", i), "synthetic", fmt.Sprintf("synthetic-%02d", i), "synthetic.Large", "00000000-0000-0000-0000-000000000001", "2026-09-25 12:09:16+00:00", "2026-09-25 12:09:16+00:00")
			must(err)
		}
		must(tx.Commit())
		fmt.Printf("seeded blob_bytes=%d deliveries=51\n", len(blob))
		return
	}
	start := time.Now()
	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			cg, _ := os.ReadFile("/sys/fs/cgroup/memory.current")
			fmt.Printf("t=%.1f heap_mib=%d sys_mib=%d handlers=%d cgroup_bytes=%s", time.Since(start).Seconds(), m.HeapAlloc>>20, m.Sys>>20, calls.Load(), cg)
			time.Sleep(time.Second)
		}
	}()
	go func() {
		for e := range o.Errors() {
			fmt.Printf("OUTBOX_ERROR %v\n", e)
		}
	}()
	fmt.Println("START_CHECKED")
	must(o.StartChecked())
	fmt.Println("STARTED")
	deadline := time.After(150 * time.Second)
	for {
		select {
		case <-deadline:
			fmt.Println("TIMEOUT")
			os.Exit(2)
		case <-time.After(250 * time.Millisecond):
			if calls.Load() == 51 {
				var n int
				must(db.QueryRow("SELECT count(*) FROM outbox_deliveries").Scan(&n))
				if n == 0 {
					runtime.GC()
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					fmt.Printf("PASS handlers=51 remaining=0 elapsed=%s retained_heap_mib=%d\n", time.Since(start), m.HeapAlloc>>20)
					must(o.Close())
					return
				}
			}
		}
	}
}
