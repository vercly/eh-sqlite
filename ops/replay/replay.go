// Package replay implements offline dead-letter requeue into outbox/async_tasks.
// Operator tooling defaults to dry-run; Apply requires an actor identity.
package replay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vercly/eh-sqlite/internal/deadletter"
	outboxsqlite "github.com/vercly/eh-sqlite/outbox/sqlite"
	"github.com/vercly/eh-sqlite/schema"
	"github.com/vercly/eventhorizon/uuid"
)

const (
	SourceOutbox   = "outbox"
	SourceCommand  = "command"
	HandlerNoMatch = "no_match"
)

// Options controls a replay batch. Apply defaults false (dry-run).
type Options struct {
	// Apply commits changes. When false, only plans actions.
	Apply bool
	// Actor is required when Apply is true (audit replayed_by).
	Actor string
	// IDs limits work to explicit dead_letters.id values. Required for Apply
	// of more than zero rows unless AllowBroad is set (CLI keeps this false).
	IDs []string
	// Source filters by dead_letters.source when non-empty.
	Source string
	// AllowBroad permits Apply without IDs (not used by default CLI).
	AllowBroad bool
}

// ItemStatus is the outcome for one dead-letter row.
type ItemStatus string

const (
	StatusWouldReplay     ItemStatus = "would_replay"
	StatusReplayed        ItemStatus = "replayed"
	StatusAlreadyReplayed ItemStatus = "already_replayed"
	StatusSkipped         ItemStatus = "skipped"
	StatusError           ItemStatus = "error"
)

// PlanItem is one planned or applied action.
type PlanItem struct {
	DeadLetterID string     `json:"dead_letter_id"`
	Source       string     `json:"source"`
	HandlerType  string     `json:"handler_type"`
	OutboxID     string     `json:"outbox_id,omitempty"`
	Action       string     `json:"action"`
	Status       ItemStatus `json:"status"`
	Detail       string     `json:"detail,omitempty"`
}

// Result summarizes a replay run.
type Result struct {
	DryRun          bool       `json:"dry_run"`
	Actor           string     `json:"actor,omitempty"`
	Items           []PlanItem `json:"items"`
	Replayed        int        `json:"replayed"`
	AlreadyReplayed int        `json:"already_replayed"`
	// Missing counts explicit --id values that were not found in dead_letters.
	Missing int `json:"missing"`
	Errors  int `json:"errors"`
}

// ValidateRequiredTables is a read-only check that a real event-store schema is
// present. It never CREATE/ALTER. Dry-run uses this only.
func ValidateRequiredTables(db *sql.DB) error {
	for _, name := range []string{"dead_letters", "async_tasks"} {
		ok, err := deadletter.TableExists(db, name)
		if err != nil {
			return fmt.Errorf("replay: check table %s: %w", name, err)
		}
		if !ok {
			return fmt.Errorf("replay: required table %q is missing (refusing to create schema stubs; use a real event-store database)", name)
		}
	}
	v2, err := isV2(db)
	if err != nil {
		return err
	}
	if v2 {
		for _, name := range []string{"outbox_publications", "outbox_deliveries"} {
			ok, err := deadletter.TableExists(db, name)
			if err != nil {
				return fmt.Errorf("replay: check table %s: %w", name, err)
			}
			if !ok {
				return fmt.Errorf("replay: required table %q is missing", name)
			}
		}
		return nil
	}
	ok, err := deadletter.TableExists(db, "outbox")
	if err != nil {
		return fmt.Errorf("replay: check table outbox: %w", err)
	}
	if !ok {
		return fmt.Errorf("replay: required legacy table \"outbox\" is missing")
	}
	return nil
}

// MigrateSchema adds dead_letters.replayed_at / replayed_by when missing.
// Call only on --apply. Dry-run must not invoke this.
func MigrateSchema(db *sql.DB) error {
	if err := ValidateRequiredTables(db); err != nil {
		return err
	}
	for _, col := range []struct {
		name string
		decl string
	}{
		{"replayed_at", "TIMESTAMP"},
		{"replayed_by", "TEXT"},
	} {
		if err := deadletter.AddColumnIfAbsent(db, "dead_letters", col.name, col.decl); err != nil {
			return fmt.Errorf("replay: migrate dead_letters.%s: %w", col.name, err)
		}
	}
	return nil
}

// EnsureSchema is MigrateSchema (validate + additive replay columns). Prefer
// ValidateRequiredTables for dry-run and MigrateSchema only on apply.
func EnsureSchema(db *sql.DB) error {
	return MigrateSchema(db)
}

// ReplayDeadLetters plans or applies offline DLQ requeue. Each applied row is
// handled in its own SQLite transaction (restore + mark replayed_at/by).
//
// Dry-run is schema- and data-preserving: it only validates required tables
// exist and plans actions. Missing replayed_* columns are treated as NULL
// (never replayed). --apply migrates those columns then commits restores.
func ReplayDeadLetters(ctx context.Context, db *sql.DB, opts Options) (Result, error) {
	// Trim/dedupe before apply validation so whitespace-only --id cannot open
	// a broad apply after cleanup reduces IDs to zero.
	opts.IDs = dedupeIDsPreserveOrder(opts.IDs)

	if opts.Apply {
		if strings.TrimSpace(opts.Actor) == "" {
			return Result{}, errors.New("replay: --actor is required with --apply")
		}
		if len(opts.IDs) == 0 && !opts.AllowBroad {
			return Result{}, errors.New("replay: --apply requires explicit --id (no broad default)")
		}
		if err := MigrateSchema(db); err != nil {
			return Result{}, err
		}
	} else {
		if err := ValidateRequiredTables(db); err != nil {
			return Result{}, err
		}
	}

	hasReplayCols, err := hasReplayAuditColumns(db)
	if err != nil {
		return Result{}, err
	}

	rows, err := selectDeadLetters(ctx, db, opts, hasReplayCols)
	if err != nil {
		return Result{}, err
	}

	res := Result{DryRun: !opts.Apply, Actor: opts.Actor}

	// Explicit IDs: preserve operator order and surface missing ids (never a
	// silent empty success when the operator named a non-existent row).
	if len(opts.IDs) > 0 {
		byID := make(map[string]dlqRow, len(rows))
		for _, row := range rows {
			byID[row.ID] = row
		}
		for _, id := range opts.IDs {
			row, ok := byID[id]
			if !ok {
				res.Missing++
				res.Errors++
				res.Items = append(res.Items, PlanItem{
					DeadLetterID: id,
					Action:       "none",
					Status:       StatusSkipped,
					Detail:       "dead letter id not found",
				})
				continue
			}
			item, err := replayOne(ctx, db, opts, row)
			if err != nil {
				item.Status = StatusError
				item.Detail = err.Error()
				res.Errors++
			} else {
				switch item.Status {
				case StatusReplayed:
					res.Replayed++
				case StatusAlreadyReplayed:
					res.AlreadyReplayed++
				}
			}
			res.Items = append(res.Items, item)
		}
		return res, nil
	}

	for _, row := range rows {
		item, err := replayOne(ctx, db, opts, row)
		if err != nil {
			item.Status = StatusError
			item.Detail = err.Error()
			res.Errors++
		} else {
			switch item.Status {
			case StatusReplayed:
				res.Replayed++
			case StatusAlreadyReplayed:
				res.AlreadyReplayed++
			}
		}
		res.Items = append(res.Items, item)
	}
	return res, nil
}

type dlqRow struct {
	ID             string
	Source         string
	EventType      string
	AggregateID    string
	HandlerType    string
	OutboxID       sql.NullString
	Blob           string
	RetryCount     int
	CreatedAt      time.Time
	ReplayedAt     sql.NullTime
	ReplayedBy     sql.NullString
	PublicationID  sql.NullString
	LegacyOutboxID sql.NullString
}

func hasReplayAuditColumns(db *sql.DB) (bool, error) {
	at, err := deadletter.ColumnExists(db, "dead_letters", "replayed_at")
	if err != nil {
		return false, err
	}
	by, err := deadletter.ColumnExists(db, "dead_letters", "replayed_by")
	if err != nil {
		return false, err
	}
	return at && by, nil
}

func selectDeadLetters(ctx context.Context, db *sql.DB, opts Options, hasReplayCols bool) ([]dlqRow, error) {
	// Old schemas without replayed_* treat markers as NULL (compatible dry-run).
	replaySelect := `NULL AS replayed_at, NULL AS replayed_by`
	if hasReplayCols {
		replaySelect = `replayed_at, replayed_by`
	}
	provenanceSelect := `NULL AS publication_id, NULL AS legacy_outbox_id`
	hasPublicationID, err := deadletter.ColumnExists(db, "dead_letters", "publication_id")
	if err != nil {
		return nil, err
	}
	hasLegacyOutboxID, err := deadletter.ColumnExists(db, "dead_letters", "legacy_outbox_id")
	if err != nil {
		return nil, err
	}
	if hasPublicationID && hasLegacyOutboxID {
		provenanceSelect = `publication_id, legacy_outbox_id`
	}
	q := fmt.Sprintf(`
		SELECT id, source, event_type, aggregate_id, handler_type, outbox_id, blob,
		       retry_count, created_at, %s, %s
		FROM dead_letters WHERE 1=1`, replaySelect, provenanceSelect)
	var args []any
	if opts.Source != "" {
		q += ` AND source = ?`
		args = append(args, opts.Source)
	}
	if len(opts.IDs) > 0 {
		placeholders := make([]string, len(opts.IDs))
		for i, id := range opts.IDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		q += ` AND id IN (` + strings.Join(placeholders, ",") + `)`
	}
	q += ` ORDER BY dead_at ASC, id ASC`

	rs, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("replay: query dead_letters: %w", err)
	}
	defer rs.Close()

	var out []dlqRow
	for rs.Next() {
		var r dlqRow
		if err := rs.Scan(&r.ID, &r.Source, &r.EventType, &r.AggregateID, &r.HandlerType, &r.OutboxID,
			&r.Blob, &r.RetryCount, &r.CreatedAt, &r.ReplayedAt, &r.ReplayedBy, &r.PublicationID, &r.LegacyOutboxID); err != nil {
			return nil, fmt.Errorf("replay: scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

func replayOne(ctx context.Context, db *sql.DB, opts Options, row dlqRow) (PlanItem, error) {
	item := PlanItem{
		DeadLetterID: row.ID,
		Source:       row.Source,
		HandlerType:  row.HandlerType,
	}
	if row.OutboxID.Valid {
		item.OutboxID = row.OutboxID.String
	}

	if row.ReplayedAt.Valid {
		item.Status = StatusAlreadyReplayed
		item.Action = "none"
		item.Detail = "already marked replayed"
		return item, nil
	}

	switch {
	case row.Source == SourceOutbox && row.HandlerType == HandlerNoMatch:
		item.Action = "outbox_rematch_row"
		item.Detail = "insert outbox row with empty handlers for re-match"
	case row.Source == SourceOutbox:
		item.Action = "outbox_restore_handler"
		item.Detail = "restore handler_type onto outbox row (or recreate)"
	case row.Source == SourceCommand:
		item.Action = "command_async_task"
		item.Detail = "insert async_tasks status=new for durable resume"
	default:
		item.Status = StatusSkipped
		item.Action = "unsupported"
		item.Detail = fmt.Sprintf("unsupported source %q", row.Source)
		return item, nil
	}
	// v2 replay must decode the stored routing envelope to retain partition
	// semantics. Do this during dry-run too, so its plan is actionable and an
	// apply does not discover malformed payloads after the operator approves it.
	if v2, err := isV2(db); err != nil {
		return item, err
	} else if v2 && row.Source == SourceOutbox {
		if _, err := replayPartitionKey(row.Blob); err != nil {
			return item, fmt.Errorf("decode replay partition key: %w", err)
		}
	}

	if !opts.Apply {
		item.Status = StatusWouldReplay
		return item, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return item, err
	}
	defer func() { _ = tx.Rollback() }()

	// Re-check replayed_at inside TX for concurrency.
	var replayed sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT replayed_at FROM dead_letters WHERE id = ?`, row.ID).Scan(&replayed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			item.Status = StatusSkipped
			item.Detail = "dead letter row missing"
			return item, nil
		}
		return item, err
	}
	if replayed.Valid {
		item.Status = StatusAlreadyReplayed
		item.Action = "none"
		item.Detail = "already marked replayed"
		return item, nil
	}

	switch item.Action {
	case "outbox_rematch_row":
		if err := restoreNoMatch(ctx, tx, row); err != nil {
			return item, err
		}
	case "outbox_restore_handler":
		if err := restoreTerminalHandler(ctx, tx, row); err != nil {
			return item, err
		}
	case "command_async_task":
		if err := restoreCommandTask(ctx, tx, row); err != nil {
			return item, err
		}
	}

	now := schema.UTC(time.Now())
	res, err := tx.ExecContext(ctx, `
		UPDATE dead_letters SET replayed_at = ?, replayed_by = ? WHERE id = ? AND replayed_at IS NULL
	`, now, opts.Actor, row.ID)
	if err != nil {
		return item, fmt.Errorf("mark replayed: %w", err)
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		item.Status = StatusAlreadyReplayed
		item.Detail = "concurrent replay won"
		return item, nil
	}
	if err := tx.Commit(); err != nil {
		return item, err
	}
	item.Status = StatusReplayed
	return item, nil
}

func restoreNoMatch(ctx context.Context, tx *sql.Tx, row dlqRow) error {
	v2, err := isV2Tx(ctx, tx)
	if err != nil {
		return err
	}
	if v2 {
		return restoreV2(ctx, tx, row, "")
	}
	// Empty handlers list is the rematch sentinel (see outbox planAndClaim).
	outboxID := row.ID
	if row.OutboxID.Valid && row.OutboxID.String != "" {
		outboxID = row.OutboxID.String
	}
	now := schema.UTC(time.Now())
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count)
		VALUES (?, ?, ?, ?, ?, NULL, '[]', ?, 0)
		ON CONFLICT(id) DO UPDATE SET
			handlers = '[]',
			taken_at = NULL,
			available_at = excluded.available_at,
			retry_count = 0,
			event_blob = excluded.event_blob
	`, outboxID, row.EventType, row.AggregateID, row.CreatedAt, now, row.Blob)
	return err
}

func restoreTerminalHandler(ctx context.Context, tx *sql.Tx, row dlqRow) error {
	v2, err := isV2Tx(ctx, tx)
	if err != nil {
		return err
	}
	if v2 {
		return restoreV2(ctx, tx, row, row.HandlerType)
	}
	if !row.OutboxID.Valid || row.OutboxID.String == "" {
		return errors.New("terminal outbox dead letter missing outbox_id")
	}
	outboxID := row.OutboxID.String
	handler := row.HandlerType
	now := schema.UTC(time.Now())

	var handlersBlob string
	err = tx.QueryRowContext(ctx, `SELECT handlers FROM outbox WHERE id = ?`, outboxID).Scan(&handlersBlob)
	if errors.Is(err, sql.ErrNoRows) {
		// Recreate row with only the failed handler. Always start a fresh
		// attempt: retry_count=0 and available_at=now (never carry the DLQ's
		// exhausted retry_count or a delayed available_at).
		handlersJSON, _ := json.Marshal([]string{handler})
		_, err = tx.ExecContext(ctx, `
			INSERT INTO outbox (id, event_type, aggregate_id, created_at, available_at, taken_at, handlers, event_blob, retry_count)
			VALUES (?, ?, ?, ?, ?, NULL, ?, ?, 0)
		`, outboxID, row.EventType, row.AggregateID, row.CreatedAt, now, string(handlersJSON), row.Blob)
		return err
	}
	if err != nil {
		return err
	}

	var handlers []string
	if err := json.Unmarshal([]byte(handlersBlob), &handlers); err != nil {
		return fmt.Errorf("parse handlers: %w", err)
	}
	found := false
	for _, h := range handlers {
		if h == handler {
			found = true
			break
		}
	}
	if !found {
		handlers = append(handlers, handler)
	}
	newBlob, err := json.Marshal(handlers)
	if err != nil {
		return err
	}
	// Fresh attempt on existing row: clear taken_at, reset retry budget, make
	// due immediately so the processor does not wait on a stale available_at.
	_, err = tx.ExecContext(ctx, `
		UPDATE outbox SET handlers = ?, taken_at = NULL,
			available_at = ?, retry_count = 0
		WHERE id = ?
	`, string(newBlob), now, outboxID)
	return err
}

// restoreV2 recreates one publication plus either a terminal recipient or a
// NULL rematch sentinel. DLQ payloads are self-contained, so no legacy row is
// consulted. New IDs avoid conflating repeated publications of one EH event.
func restoreV2(ctx context.Context, tx *sql.Tx, row dlqRow, handler string) error {
	publicationID := uuid.New().String()
	deliveryID := uuid.New().String()
	now := schema.UTC(time.Now())
	partitionKey, err := replayPartitionKey(row.Blob)
	if err != nil {
		return fmt.Errorf("decode replay partition key: %w", err)
	}
	legacyOutboxID := any(nil)
	if row.LegacyOutboxID.Valid && row.LegacyOutboxID.String != "" {
		legacyOutboxID = row.LegacyOutboxID.String
	} else if !row.PublicationID.Valid && row.OutboxID.Valid && row.OutboxID.String != "" {
		legacyOutboxID = row.OutboxID.String
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_publications (publication_id, event_type, aggregate_id, partition_key, event_blob, created_at, origin, origin_ref)
		VALUES (?, ?, ?, ?, ?, ?, 'replay', ?)`, publicationID, row.EventType, row.AggregateID, partitionKey, row.Blob, now, row.ID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox_deliveries (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id, created_at, available_at, taken_at, retry_count, unresolved_at, legacy_outbox_id)
		VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, NULL, 0, NULL, ?)`, deliveryID, publicationID, nullableHandler(handler), row.EventType, row.AggregateID, now, now, legacyOutboxID)
	return err
}

func replayPartitionKey(blob string) (string, error) {
	return outboxsqlite.StoredEventPartitionKey([]byte(blob))
}

func nullableHandler(handler string) any {
	if handler == "" {
		return nil
	}
	return handler
}

func isV2(db *sql.DB) (bool, error) {
	ok, err := deadletter.TableExists(db, "eh_sqlite_migrations")
	if err != nil || !ok {
		return false, err
	}
	var n int
	err = db.QueryRow(`SELECT COUNT(*) FROM eh_sqlite_migrations WHERE component = 'outbox' AND name = 'v2_deliveries'`).Scan(&n)
	return n > 0, err
}
func isV2Tx(ctx context.Context, tx *sql.Tx) (bool, error) {
	var tableCount int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'eh_sqlite_migrations'`).Scan(&tableCount); err != nil {
		return false, err
	}
	if tableCount == 0 {
		return false, nil
	}
	var n int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM eh_sqlite_migrations WHERE component = 'outbox' AND name = 'v2_deliveries'`).Scan(&n)
	return n > 0, err
}

func restoreCommandTask(ctx context.Context, tx *sql.Tx, row dlqRow) error {
	// Deterministic task_uuid from dead letter id for idempotent re-insert.
	taskUUID := "dlq-replay-" + row.ID
	now := schema.UTC(time.Now())
	_, err := tx.ExecContext(ctx, `
		INSERT INTO async_tasks (task_uuid, command_type, command_blob, status, retry_count, max_retries, created_at, updated_at)
		VALUES (?, ?, ?, 'new', 0, 5, ?, ?)
		ON CONFLICT(task_uuid) DO UPDATE SET
			status = 'new',
			command_blob = excluded.command_blob,
			command_type = excluded.command_type,
			retry_count = 0,
			locked_by = NULL,
			locked_at = NULL,
			next_retry_at = NULL,
			updated_at = excluded.updated_at,
			last_error = NULL
	`, taskUUID, row.EventType, row.Blob, now, now)
	return err
}

// dedupeIDsPreserveOrder drops empty and duplicate IDs, keeping first occurrence order.
func dedupeIDsPreserveOrder(ids []string) []string {
	if len(ids) == 0 {
		return ids
	}
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
