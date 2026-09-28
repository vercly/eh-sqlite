# Payload admission

The default byte budget is 256 MiB of serialized event envelopes, in addition
to the existing record and per-key limits. Configure it with
`WithPayloadBudget(bytes)` (positive int64). Each recipient is charged separately:
its codec and handler receive independent objects, and Match still receives the
full event. This avoids changing custom data-dependent matchers.

Candidate and sentinel queries return IDs and `octet_length(event_blob)` only.
SQLite can obtain this byte count from record metadata without reading overflow
pages. The bundled mattn SQLite supports this function (SQLite >= 3.43 required
if building against a system SQLite). Payload is fetched only after a nonblocking
reservation succeeds. Blocked candidates preserve FIFO within their key; other
keys can progress. A byte-blocked rematch sentinel cannot be overtaken by normal
claims. Workers release reservations after finalize/export, or on abandonment;
claim rollback releases all reservations. Queue removal clears the backing-array
pointer, and finished deliveries drop event/body/context references.

A payload larger than the entire budget stays in its original publication with
its exact remaining recipients. Those deliveries receive `unresolved_at` and
lose `dispatch_key`, plus `ErrPayloadTooLarge` on Errors() and the
`payload_too_large` skip metric. No implicit deletion or DLQ copy occurs. This
uses the existing unresolved representation, not a new quarantine table; inspect
the byte count and error to distinguish it from a missing handler. Raising the
budget or externally repairing the payload followed by restart reconsiders it.
Startup partition reconciliation skips blobs exceeding the budget; their stored
partition is retained until they can be processed. No new schema is required.

The byte budget is NOT an RSS limit. SQLite/native allocations, JSON decoding,
GC headroom and application allocations add overhead. Handler code can retain
an event beyond return and remains responsible for that memory. Diagnostic
OutboxError.Event values for envelopes above 64 KiB retain event identity and bounded short string metadata,
not the body, so the buffered error channel cannot keep large payloads alive
after finalize. Full records remain available in the publication/DLQ as applicable.
Publish-time caller allocations and large DLQ exports are not independently
byte-budgeted; producers should store large content outside transport messages.

## Validation, 2026-09-28

Local synthetic regression uses one ~156 MB envelope, 51 separate recipients,
and no-op successful handlers (no PWG parsing or network). Original v1.2.0 OOMs
before any handler with admission=50 under both 2 and 4 GiB container limits.
Admission=1 alone still OOMs after 12 finalized recipients under 4 GiB.
With byte admission and reference release, admission=50 completes all 51 under
2 GiB with GOMEMLIMIT=1GiB in ~16s; sampled cgroup usage is approximately 1.0–1.5
GiB. This is a synthetic transport check, not a full ORLEN report benchmark.

A second run with the normal Go GC (no GOMEMLIMIT), 4 GiB container cap and
admission=50 completed 51/51 in 17.7s, sampled cgroup usage <=1.83 GB.
The runnable fixture is `cmd/outbox-payload-repro`; always run large cases in a
memory-limited container, with no network. `-mode seed` creates a synthetic DB;
`-mode run` processes it. `-bytes 1048576` seeds the small control. `-db` selects
the fixture path (default /data/repro.db). Seed into an empty path and copy the
closed DB for each scenario. The harness reports Go heap, cgroup memory and
handler count; Docker's OOMKilled/ExitCode are the authoritative OOM evidence.

Final code rerun (shared decode byte buffer, diagnostic bounds included):
2 GiB container, no GOMEMLIMIT, admission=50, 51/51 finalized in 15.74s;
Docker ExitCode=0, OOMKilled=false. Sampled cgroup usage stayed below 1.71 GB.
Live heap after one forced GC was 298 MiB (codec/runtime buffer retention is
outside the serialized admission accounting); this does not grow per recipient.

Scheduling keeps FIFO per dispatch key. The first byte-blocked head reserves
future capacity: smaller deliveries may use the remaining spare capacity, but
cannot repeatedly fill the space it needs. Existing workers drain naturally;
the protected head fits on the next eligible pass once enough bytes are freed. For legacy base64
envelopes, the raw JSON string and decoded metadata can retain about twice the
charged byte count, plus temporary decoder buffers and handler allocations.
Large diagnostics retain at most 16 short string metadata values (including
normal correlation/routing fields), never nested payloads or large strings.

Rematch sentinels take priority over a protected normal head to avoid a barrier
deadlock. The normal head can register its protection again after rematching.
