# eh-sqlite findings and repair plan

## Scope

Review target: local `eh-sqlite` implementation and its integration through
`internal/ehinfa/eventstore`.

Checked areas:
- SQLite event store (`eh-sqlite/eventstore/sqlite`)
- SQLite outbox (`eh-sqlite/outbox/sqlite`)
- durable command middleware (`eh-sqlite/middleware/commandhandler/durable`)
- tracing helpers and current Vercly wiring

Verification performed:
- `GOWORK=off go test ./...` in `eh-sqlite` passes.
- `make` from repository root passes.
- Plain `go test ./...` from `eh-sqlite` does not work because root `go.work`
  does not include the `eh-sqlite` module.

## Implementation status (updated 2026-06-25)

The findings below were validated against the code and confirmed accurate. The
correctness core has now been implemented and tested:

- ✅ Finding 1 / Phase 1 — durable resume lifecycle (implemented + tested)
- ✅ Finding 2 / Phase 2 — atomic outbox claiming (implemented + tested)
- ✅ Finding 3 / Phase 3 — event-store writer serialization (implemented + tested)
- ✅ Finding 6 / Phase 5 — DB ownership cleanup (implemented + tested)
- ⏳ Finding 4 / Phase 4 — durable retry semantics (pending; not started)
- ⏳ Finding 5 / Phase 6 — outbox throughput tuning + metrics (pending; not started)

Verification after implementation: `GOWORK=off go test ./...` (incl. `-race`) in
`eh-sqlite` passes; `go build ./...` and `make` from the repository root pass.

## Additional findings (from validation)

These were discovered while validating the report and are not covered by the
original six findings.

### A. `EventStoreDSN` lacked `_txlock=immediate` — ✅ FIXED

`internal/config/database.go` defined `EventStoreDSN` without `_txlock=immediate`,
while `DBURI` and `EventsDBURI` both include it. With the mattn driver this means
`BeginTx` issued `BEGIN DEFERRED`, which is exactly what enabled the read-then-write
race in Finding 3. Fixed by adding `_txlock=immediate` (defense-in-depth alongside
the atomic `$all` increment).

### B. Incomplete mattn → modernc driver migration — open

Commit `51e8121` ("replace mattn/go-sqlite3 with modernc.org/sqlite") is only
partially done for the production path. The real event-store connection
(`internal/db/connection.go`) still uses `mattn` (`sql.Open("sqlite3", ...)`). The
`eh-sqlite` packages only blank-import `modernc.org/sqlite` (registering the unused
`"sqlite"` driver), and the `addTimestampParams` helpers in
`eventstore/sqlite/db_helper.go` and `outbox/sqlite/db_helper.go` are dead code
(no non-test callers). Not blocking, but worth resolving so the runtime and tests
use the same driver.

### C. Finding 1 is worse than first described — confirmed (now fixed)

Each restart not only created a duplicate row: `Resume` marked the original row
`processing` (its completion callback was lost across the restart, so it stayed
`processing` forever) and the duplicate stayed `new` forever (the command hit a
handler-less bus). The next restart then re-selected both `new` and `processing`
rows, snowballing the `async_tasks` table on every restart. The Phase 1 fix removes
this entirely (row reuse + resume after handler registration).

## Findings

### 1. Durable resume runs before command handlers are registered

Severity: critical — Status: ✅ FIXED (Phase 1)

`internal/ehinfa/eventstore.NewEhInfra` starts infrastructure and calls
`durable.Resume(...)` before `cmd/services/vercly/setupeh.go` registers the
domain handlers with `SetupSaga` and `SetupHandler`.

Relevant code:
- `internal/ehinfa/eventstore/setup.go:75`
- `internal/ehinfa/eventstore/setup.go:78`
- `cmd/services/vercly/setupeh.go:30`
- `cmd/services/vercly/setupeh.go:42`

Impact:
- Commands resumed after restart can hit a command bus with no handlers yet.
- `Resume` currently receives the wrapped bus. The wrapped bus includes durable
  middleware, so resumed commands can be persisted as new `async_tasks` rows
  instead of completing the original row.
- This can create stuck tasks, duplicate task records, or lost recovery after
  restart.

### 2. Outbox claim is not atomic across processes

Severity: high — Status: ✅ FIXED (Phase 2)

`fetchAndLockEvents` first selects available outbox rows and then updates
`taken_at` row by row. The update does not verify that the row is still
available.

Relevant code:
- `eh-sqlite/outbox/sqlite/outbox.go:156`
- `eh-sqlite/outbox/sqlite/outbox.go:494`
- `eh-sqlite/outbox/sqlite/outbox.go:501`
- `eh-sqlite/outbox/sqlite/outbox.go:522`

Impact:
- Two app instances can select the same rows before either commits `taken_at`.
- Both can dispatch handlers for the same event.
- This breaks outbox at-least-once expectations by making duplicate dispatch
  much more likely under multi-instance deployments.

### 3. Event store global position uses read-then-insert sequencing

Severity: high — Status: ✅ FIXED (Phase 3)

`Save` reads `$all.position`, calculates positions in Go, inserts events with
explicit `position`, and updates `$all` afterward.

Relevant code:
- `eh-sqlite/eventstore/sqlite/eventstore.go:196`
- `eh-sqlite/eventstore/sqlite/eventstore.go:227`
- `eh-sqlite/eventstore/sqlite/eventstore.go:231`
- `eh-sqlite/eventstore/sqlite/eventstore.go:245`

Impact:
- Concurrent saves for different aggregate IDs can read the same global
  position and attempt to insert the same event positions.
- In one process the current integration mostly masks this with
  `SetMaxOpenConns(1)`, but that is a throughput bottleneck and does not
  protect against multiple app instances.
- Failures would surface as primary key conflicts rather than clean event-store
  conflict handling.

### 4. Durable retry fields are not implemented as behavior

Severity: medium — Status: ⏳ pending

The `async_tasks` table has `failed_retriable`, `retry_count`, `max_retries`,
and `next_retry_at`, but the runtime does not use them as a complete retry
state machine.

Relevant code:
- `eh-sqlite/middleware/commandhandler/durable/durable.go:59`
- `eh-sqlite/middleware/commandhandler/durable/durable.go:64`
- `eh-sqlite/middleware/commandhandler/durable/durable.go:152`
- `eh-sqlite/tracing/durable.go:50`

Impact:
- All command handler errors are currently recorded as `failed_permanent`.
- `Resume` only selects `new` and `processing` tasks.
- The schema suggests retry/backoff support that does not actually exist yet.

### 5. Throughput is constrained by one DB connection and serialized outbox work

Severity: medium — Status: ⏳ pending

The event-store infrastructure creates the SQLite connection through
`internal/db.NewDBConnection`, which defaults to one open connection. The outbox
also processes fixed-size batches and serializes DB updates.

Relevant code:
- `internal/db/connection.go:24`
- `eh-sqlite/outbox/sqlite/outbox.go:75`
- `eh-sqlite/outbox/sqlite/outbox.go:156`
- `eh-sqlite/outbox/sqlite/outbox.go:537`

Impact:
- One connection reduces SQLite lock errors, but it serializes event store,
  outbox, and durable command DB access in one process.
- Outbox throughput is limited by `LIMIT 50`, serial update/delete statements,
  and a fixed sleep between batches.
- This may be acceptable for low volume, but it will become a bottleneck under
  larger event bursts.

### 6. DB ownership is unclear during shutdown

Severity: low — Status: ✅ FIXED (Phase 5)

`Outbox.Close`, `EventStore.Close`, and `EhInfra.Stop` all close the same
external `*sql.DB`.

Relevant code:
- `eh-sqlite/outbox/sqlite/outbox.go:303`
- `eh-sqlite/eventstore/sqlite/eventstore.go:431`
- `internal/ehinfa/eventstore/setup.go:99`

Impact:
- Current shutdown may tolerate repeated closes, but ownership is ambiguous.
- This makes future reuse, partial shutdown, and tests more fragile.

## Repair plan

### Phase 1: fix durable command resume lifecycle — ✅ DONE

1. Remove `durable.Resume(...)` from `internal/ehinfa/eventstore.NewEhInfra`.
2. Add an explicit `ResumeDurableCommands(ctx)` method on `EhInfra`.
3. Call `ResumeDurableCommands` from `ConfigureEventStore` only after
   `SetupSaga` and `SetupHandler` complete.
4. Ensure resumed commands are dispatched without passing through durable
   persistence again.

Recommended implementation shape:
- Keep `wrappedCommandBus` for public command handling.
- Add a replay bus path that can still use async execution if needed, but skips
  durable middleware.
- Alternatively add context support for an existing durable task ID so the
  middleware updates the existing row instead of inserting a new one.

Required tests:
- A task in `new` status is resumed after handlers are registered.
- Resume does not insert a second `async_tasks` row for the same command.
- Resume before handler registration is impossible through the public setup
  flow, or returns a clear error in a unit test.

### Phase 2: make outbox row claiming atomic — ✅ DONE

Replace select-then-update claiming with an atomic claim operation.

Preferred SQLite 3.35+ approach:

```sql
UPDATE outbox
SET taken_at = ?
WHERE id IN (
    SELECT id
    FROM outbox
    WHERE taken_at IS NULL OR taken_at < ?
    ORDER BY created_at ASC
    LIMIT ?
)
RETURNING id, event_type, aggregate_id, created_at, taken_at, handlers, event_blob, retry_count;
```

Fallback approach:

```sql
UPDATE outbox
SET taken_at = ?
WHERE id = ?
  AND (taken_at IS NULL OR taken_at < ?);
```

When using the fallback, check `RowsAffected() == 1` before dispatching a row.

Also add a claim-oriented index:

```sql
CREATE INDEX IF NOT EXISTS idx_outbox_claim
ON outbox (taken_at, created_at);
```

Required tests:
- Two `Outbox` instances connected to the same database do not dispatch the
  same row twice.
- A stale `taken_at` row becomes claimable again after `PeriodicSweepAge`.
- A fresh `taken_at` row is not claimed.

### Phase 3: serialize event-store global position safely — ✅ DONE

First pragmatic fix:
- Use `BEGIN IMMEDIATE` for `EventStore.Save` so the writer lock is acquired
  before reading `$all.position`.
- Keep optimistic aggregate version checks as they are.

Longer-term improvement:
- Stop manually calculating event positions before insert.
- Let SQLite allocate `position` with `AUTOINCREMENT`, or use an atomic counter
  update with `RETURNING` to reserve a contiguous range.

Required tests:
- Concurrent saves for different aggregate IDs do not fail with duplicate
  global positions.
- Concurrent saves for the same aggregate still return an event conflict.
- Multi-event saves reserve contiguous positions.

### Phase 4: decide and implement durable retry semantics — ⏳ pending

Choose one of two paths.

Path A: implement retry support:
- Classify command errors into retriable and permanent errors.
- On retriable error, set `status = 'failed_retriable'`, increment
  `retry_count`, set `next_retry_at`, and preserve `last_error`.
- Make `Resume` select due retriable tasks:

```sql
WHERE status IN ('new', 'processing')
   OR (
      status = 'failed_retriable'
      AND next_retry_at <= CURRENT_TIMESTAMP
      AND retry_count < max_retries
   )
```

Path B: simplify the schema and naming:
- Remove or stop exposing retry-related columns until retry behavior exists.
- Keep only `new`, `processing`, `completed`, and `failed_permanent`.

Recommended path: implement retry support, because the schema and API already
point in that direction.

Required tests:
- Retriable failures move to `failed_retriable` with `next_retry_at`.
- Due retriable tasks are resumed.
- Exhausted tasks become permanent failures and are not retried.

### Phase 5: clarify DB ownership — ✅ DONE

Change component close behavior:
- `Outbox.Close` closes its goroutine and prepared statements only.
- `EventStore.Close` closes prepared statements only.
- `EhInfra.Stop` owns and closes the shared `*sql.DB` once.

Required tests:
- Closing outbox does not make event store operations fail because the shared DB
  was closed.
- `EhInfra.Stop` can be called once without double-close side effects.

### Phase 6: reduce outbox throughput bottlenecks — ⏳ pending

After correctness fixes:
- Make batch size configurable instead of hard-coded `LIMIT 50`.
- Reconsider the fixed `10ms` sleep between batches. Keep backoff for errors or
  empty batches, but avoid sleeping during continuous backlog processing.
- Separate event concurrency from handler concurrency. `maxGoroutines` limits
  event dispatch goroutines, but each event can still spawn multiple handler
  goroutines.
- Add metrics for claimed rows, dispatched rows, handler latency, retry count,
  poison drops, and backlog size.

Required tests/benchmarks:
- Benchmark outbox throughput at different batch sizes.
- Verify backlog drains without unnecessary sleeps under continuous load.
- Verify handler concurrency limits are respected.

## Suggested implementation order

1. ✅ Durable resume lifecycle.
2. ✅ Atomic outbox claiming.
3. ✅ Event-store writer serialization (atomic `$all` increment + `_txlock=immediate`).
4. ✅ DB ownership cleanup.
5. ⏳ Durable retry semantics.
6. ⏳ Outbox throughput tuning and metrics.

This order removes the highest correctness risks first while keeping each
change relatively small and testable.
