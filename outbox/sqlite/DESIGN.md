# SQLite outbox v2: schema and lifecycle

This note is the in-tree, self-contained description of the durable layout and
the runtime lifecycle of `outbox/sqlite` after the delivery-isolation rewrite.
Read it before changing claim, finalize or migration code.

## Why v2

v1 stored one row per published event with a JSON list of recipients and
finalized the whole row only after every recipient finished. With a 64-way
fan-out one slow recipient held the record's admission slot, a full queue for
one recipient blocked the other 63, every claim pass decoded every candidate
blob, and completion of fast recipients was not durable until the slowest one
returned. Timestamps were stored with the process's local offset and compared
as text, which mis-orders rows across DST changes.

## Tables (prefix `outbox`, `WithTableName` changes the prefix)

`outbox_publications` — one row per `HandleEvent` call: `publication_id`
(uuid, distinct from the event id: the same event published twice is two
publications), `event_type`, `aggregate_id`, `partition_key` (derived at
publish from the aggregate id or correlation metadata), `event_blob`,
`created_at`, `origin` (`publish` | `migrated` | `replay`), `origin_ref`
(v1 row id or dead-letter id for provenance). A publication lives exactly as
long as one of its deliveries; the finalize that deletes the last delivery
deletes the publication.

`outbox_deliveries` — one row per recipient: `seq` (AUTOINCREMENT, the FIFO
position), `id` (uuid; this is `dead_letters.outbox_id` for v2 rows),
`publication_id`, `handler_type` (NULL = rematch sentinel), `dispatch_key`
(derived cache: handler or handler:shard; NULL when not computed or
unresolved), `dispatch_config` (fingerprint of the handler's mode/shards used
to derive the key), `event_type`, `aggregate_id`, `created_at`,
`available_at`, `taken_at`, `retry_count`, `unresolved_at`,
`legacy_outbox_id` (v1 provenance).

`dead_letters` gains `publication_id` and `legacy_outbox_id`;
`remaining_handlers` is always `[]` for v2 rows.

`eh_sqlite_migrations(component, name, applied_at)` records applied
migrations; the outbox marker is `outbox/v2_deliveries` (suffixed with the
prefix for non-default prefixes). `PRAGMA user_version` is not used.

All timestamps are written as `t.UTC()` so the driver stores canonical
`YYYY-MM-DD HH:MM:SS.fffffffff+00:00` text; comparisons bind UTC parameters.

## Lifecycle

1. `NewOutbox` creates the v2 tables and prepared statements. It never
   migrates or starts anything.
2. `AddHandler*` registers recipients while registration is open.
3. `StartChecked`:
   1. closes registration;
   2. `Migrate`: if the v1 table exists and the marker is absent, copies every
      v1 row into one publication plus one delivery per remaining handler (or
      one sentinel for `handlers=[]`), timestamps normalized, v1 order
      preserved through `seq`, then renames the v1 table to
      `<prefix>_v1_migrated` (a retained backup; never read again);
   3. one startup transaction: reset every `taken_at`, clear `unresolved_at`
      on sentinels, recompute `dispatch_key` for every delivery whose stored
      config differs from the registered handler's config, flag deliveries of
      unregistered handlers with `unresolved_at` (visible, never claimed,
      never deleted; one `ErrUnresolvedHandler` per handler type on
      `Errors()`);
   4. starts the fetcher. Any failure leaves publish blocked.
4. `HandleEvent` inserts the publication and its deliveries in one
   transaction (the caller's when provided through `context/sqlite`).
5. Claim pass (serialized by `claimMu`, one transaction):
   1. reset stale claims: rows with `taken_at` older than `PeriodicSweepAge`
      that are not admitted in this process get `taken_at = NULL` (index range
      on `taken_at IS NOT NULL AND taken_at < stale`; rare path);
   2. expand every due rematch sentinel into per-handler deliveries (all of
      them, in bounded batches, before any normal claim so an old sentinel is
      never overtaken; served by the partial index on `handler_type IS NULL`);
   3. iterate the dispatch-key ring derived from the registration (each
      handler, plus one key per shard for partitioned handlers), rotated so
      iteration starts after the previous pass's cursor. There is no scan of
      the due set to discover keys: each key is probed with one indexed
      `SELECT ... WHERE dispatch_key = ? AND taken_at IS NULL AND
      available_at <= now ORDER BY available_at, seq LIMIT q` that the claim
      index serves without sorting;
   4. rounds over the ring repeat until admission is full or a full round
      claims nothing; per round `q = min(perKey, free queue slots, free admission)` where
      `perKey = max(1, min(8, free admission / ring size))`: when admission is
      smaller than the ring, each visited key gets at most one delivery per round;
      the cursor carries rotation across passes when admission fills before
      reaching every key. Ids admitted in this process are excluded per key so a
      long-running delivery never consumes the LIMIT;
   5. each selected row is decoded, matched, reserved on its key queue,
      admitted and stamped `taken_at`. A handler that no longer resolves flags
      the row unresolved. Every reservation taken by a failed pass is released
      (a local list, independent of the returned result).
6. Execution: one worker per key pops FIFO, takes the global permit
   (`WithMaxGoroutines`, FIFO-fair across keys), runs the handler.
7. Finalize per delivery, one transaction: completed → delete (+ publication
   GC); retryable → `retry_count+1`, `available_at = now + backoff`,
   `taken_at = NULL`; fatal/exhausted → dead letter (idempotent on
   `(source, outbox_id, handler_type)`) + delete. A failing finalize is retried
   with the saved outcome (50 ms, 200 ms, 1 s); the handler is never re-run.
   After exhaustion the claim is released (`taken_at = NULL`,
   `available_at = now + PeriodicSweepAge`); if that fails too the
   `taken_at` timeout recovers the row later. A retry finalize writes the
   absolute saved `retry_count`, so re-applying it after an uncertain commit
   is idempotent. Admission is released only after this sequence, so a
   delivery never executes twice concurrently in one process. File export
   runs after commit.
8. `Close` stops admitting, lets running handlers finish and finalize, and
   abandons queued deliveries (rows keep `taken_at`; next start resets).
   `StartChecked` is serialized (`startMu`), so a concurrent second start can
   never reset claims after the first start launched the fetcher.

## Cost model

Durability is per delivery: a 64-recipient publication costs 64 finalize
transactions instead of one. The claim pass costs one transaction per wave
plus indexed probes per visited ring key. Normal selection avoids a full
due-set scan; rematch expansion and stale-claim recovery are separate paths. Under a single database connection, publishers share that connection
with claim and finalize transactions, so publish latency rises with backlog
throughput. Grouping consecutive completed outcomes of one key worker into a
single finalize transaction is the known next lever; it is deliberately not
implemented without a decision, because it couples the durability of
consecutive deliveries on one key.

## Guarantees and limits

At-least-once per recipient; completion is durable per recipient; FIFO per
dispatch key for first attempts (`available_at, seq`), retries re-enter by
eligibility time; a slow recipient no longer delays siblings' completion;
no key is starved while slots free up (round-robin with quantum), but there
is no latency or progress guarantee while admitted handlers never return;
one process per database file; no exactly-once; no ordering across keys; no
implicit remapping of historical handler identities.
