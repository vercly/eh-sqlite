// Package drain implements stop-first outbox → external broker requeue with
// publisher confirms and durable audit. Default is dry-run.
package drain

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
	ehcodec "github.com/vercly/eventhorizon/codec/json"
)

const (
	metaBodyBase64    = "legacy.body_base64"
	metaRoutingKey    = "legacy.routing_key"
	metaRoutingKeys   = "legacy.routing_keys"
	metaCorrelationID = "legacy.correlation_id"
	metaCausationID   = "legacy.causation_id"
	metaMessageID     = "legacy.message_id"
	metaOrigin        = "legacy.origin"
	metaLegacyType    = "legacy.type"
	// metaHeaderPrefix is how vercly legacyPublishMetadata stores AMQP headers
	// on EH events: legacy.header.<amqp-header-name>.
	metaHeaderPrefix = "legacy.header."
)

// safeDrainHeaderNames is the explicit allow-list of AMQP application headers
// restored from legacy.header.<name> metadata. Unknown headers are dropped
// (they may contain secrets/tokens). Keep this list tight and non-secret.
var safeDrainHeaderNames = map[string]struct{}{
	"x-vercly-instance-id":      {},
	"x-vercly-domain":           {},
	"x-vercly-original-dataset": {},
}

// ConfirmedPublisher publishes one message and returns only after broker ACK
// (or an equivalent durable confirm). Failure or missing ACK must return error.
type ConfirmedPublisher interface {
	PublishConfirmed(ctx context.Context, msg Message) error
}

// Message is a legacy Rabbit-shaped payload extracted from an outbox event blob.
type Message struct {
	OutboxID    string
	EventType   string
	Body        []byte
	RoutingKeys []string
	// Headers contains only safe, non-secret legacy fields (correlation, etc.).
	Headers map[string]any
}

// Options controls a drain run.
type Options struct {
	Apply      bool
	Actor      string
	IDs        []string
	AllowBroad bool
	// Exchange is recorded for operators; publisher implementations may use it.
	Exchange string
}

// ItemStatus is per-row outcome.
type ItemStatus string

const (
	StatusWouldDrain ItemStatus = "would_drain"
	StatusDrained    ItemStatus = "drained"
	StatusAlready    ItemStatus = "already_drained"
	StatusError      ItemStatus = "error"
	StatusSkipped    ItemStatus = "skipped"
)

// PlanItem is one planned or applied drain action.
type PlanItem struct {
	OutboxID    string     `json:"outbox_id"`
	EventType   string     `json:"event_type"`
	RoutingKeys []string   `json:"routing_keys,omitempty"`
	BodyBytes   int        `json:"body_bytes"`
	Status      ItemStatus `json:"status"`
	Detail      string     `json:"detail,omitempty"`
}

// Result summarizes a drain run.
type Result struct {
	DryRun  bool       `json:"dry_run"`
	Actor   string     `json:"actor,omitempty"`
	Items   []PlanItem `json:"items"`
	Drained int        `json:"drained"`
	Already int        `json:"already_drained"`
	// Missing counts explicit --id values with no outbox row and no drain audit.
	Missing  int    `json:"missing"`
	Errors   int    `json:"errors"`
	Aborted  bool   `json:"aborted,omitempty"`
	AbortWhy string `json:"abort_reason,omitempty"`
}

// ValidateRequiredTables is a read-only check that outbox exists. Dry-run uses
// this only — never CREATE/ALTER.
func ValidateRequiredTables(db *sql.DB) error {
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='outbox'`,
	).Scan(&n); err != nil {
		return fmt.Errorf("drain: check outbox table: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("drain: required table \"outbox\" is missing (refusing to create schema stubs; use a real event-store database)")
	}
	return nil
}

// MigrateSchema creates outbox_drain_audit when missing. Call only on --apply.
func MigrateSchema(db *sql.DB) error {
	if err := ValidateRequiredTables(db); err != nil {
		return err
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox_drain_audit (
			outbox_id TEXT PRIMARY KEY,
			drained_at TIMESTAMP NOT NULL,
			drained_by TEXT NOT NULL,
			event_type TEXT,
			routing_keys TEXT
		);
	`); err != nil {
		return fmt.Errorf("drain: ensure audit table: %w", err)
	}
	return nil
}

// EnsureSchema is MigrateSchema (validate + create audit table). Prefer
// ValidateRequiredTables for dry-run and MigrateSchema only on apply.
func EnsureSchema(db *sql.DB) error {
	return MigrateSchema(db)
}

// DrainOutbox plans or applies confirmed publish of pending outbox rows to an
// external broker. Preflights all selected rows before any publish; a single
// unsupported native-EH event aborts the whole run without publishes.
//
// Publisher contract (ConfirmedPublisher.PublishConfirmed):
//   - Must not return nil unless the message is durably accepted by a queue
//     (mandatory + confirm, or equivalent). Exchange-only ACK without a route
//     is not sufficient — unroutable must return an error so the outbox row
//     stays and commitDrain is not called.
//
// At-least-once crash window on apply:
//   - After PublishConfirmed succeeds and before commitDrain TX commits, the
//     broker may already hold the message while the outbox row still exists.
//     A crash here yields duplicate delivery on retry. After commitDrain, the
//     audit row prevents republish. No ACK / unroutable => row never deleted.
//
// Dry-run is schema- and data-preserving: only validates outbox exists and
// plans. Missing outbox_drain_audit is treated as "no prior drain". --apply
// creates the audit table then publishes.
func DrainOutbox(ctx context.Context, db *sql.DB, pub ConfirmedPublisher, opts Options) (Result, error) {
	// Trim/dedupe IDs before apply validation so whitespace-only --id cannot
	// pass len>0 and then fall through to broad apply after cleanup.
	opts.IDs = dedupeIDsPreserveOrder(opts.IDs)

	if opts.Apply {
		if strings.TrimSpace(opts.Actor) == "" {
			return Result{}, errors.New("drain: --actor is required with --apply")
		}
		if len(opts.IDs) == 0 && !opts.AllowBroad {
			return Result{}, errors.New("drain: --apply requires explicit --id (no broad default)")
		}
		if pub == nil {
			return Result{}, errors.New("drain: publisher is required with --apply")
		}
		if err := MigrateSchema(db); err != nil {
			return Result{}, err
		}
	} else {
		if err := ValidateRequiredTables(db); err != nil {
			return Result{}, err
		}
	}

	res := Result{DryRun: !opts.Apply, Actor: opts.Actor}

	// Build candidate list: explicit IDs preserve operator order and surface
	// already-drained / missing rows; broad path (dry-run only by default)
	// scans pending outbox.
	type candidate struct {
		id      string
		already bool
		missing bool
		row     outboxRow
		hasRow  bool
	}
	var candidates []candidate
	if len(opts.IDs) > 0 {
		for _, id := range opts.IDs {
			already, err := isDrained(ctx, db, id)
			if err != nil {
				return res, err
			}
			if already {
				candidates = append(candidates, candidate{id: id, already: true})
				continue
			}
			row, ok, err := loadOutbox(ctx, db, id)
			if err != nil {
				return res, err
			}
			if !ok {
				candidates = append(candidates, candidate{id: id, missing: true})
				continue
			}
			candidates = append(candidates, candidate{id: id, row: row, hasRow: true})
		}
	} else {
		rows, err := selectOutbox(ctx, db, nil)
		if err != nil {
			return Result{}, err
		}
		for _, row := range rows {
			already, err := isDrained(ctx, db, row.ID)
			if err != nil {
				return res, err
			}
			if already {
				candidates = append(candidates, candidate{id: row.ID, already: true})
				continue
			}
			candidates = append(candidates, candidate{id: row.ID, row: row, hasRow: true})
		}
	}

	// Preflight: decode every live row before any publish. One unsupported
	// native event aborts the whole run with zero side effects.
	prepared := make([]preparedRow, 0, len(candidates))
	for _, c := range candidates {
		if c.already {
			prepared = append(prepared, preparedRow{
				item: PlanItem{OutboxID: c.id, Status: StatusAlready, Detail: "drain audit exists"},
			})
			continue
		}
		if c.missing {
			// Explicit --id with neither outbox nor audit: not "already drained".
			prepared = append(prepared, preparedRow{
				item: PlanItem{OutboxID: c.id, Status: StatusSkipped, Detail: "outbox id not found"},
			})
			continue
		}
		item := PlanItem{OutboxID: c.row.ID, EventType: c.row.EventType}
		msg, err := decodeLegacyMessage(c.row)
		if err != nil {
			res.Aborted = true
			res.AbortWhy = err.Error()
			// Include prior plan items for a readable abort report.
			for _, p := range prepared {
				res.Items = append(res.Items, p.item)
				switch p.item.Status {
				case StatusAlready:
					res.Already++
				case StatusSkipped, StatusError:
					if p.item.Detail == "outbox id not found" {
						res.Missing++
					}
					res.Errors++
				}
			}
			item.Status = StatusError
			item.Detail = err.Error()
			res.Items = append(res.Items, item)
			res.Errors++
			return res, nil
		}
		item.RoutingKeys = msg.RoutingKeys
		item.BodyBytes = len(msg.Body)
		prepared = append(prepared, preparedRow{row: c.row, msg: msg, item: item})
	}

	for _, p := range prepared {
		item := p.item
		switch item.Status {
		case StatusAlready:
			res.Already++
			res.Items = append(res.Items, item)
			continue
		case StatusSkipped:
			// Explicit --id not found: count as error for non-zero CLI exit.
			if item.Detail == "outbox id not found" {
				res.Missing++
			}
			res.Errors++
			res.Items = append(res.Items, item)
			continue
		case StatusError:
			res.Errors++
			res.Items = append(res.Items, item)
			continue
		}
		if err := applyOneDrain(ctx, db, pub, opts, p.msg, &item, &res); err != nil {
			return res, err
		}
		if res.Aborted {
			return res, nil
		}
	}
	return res, nil
}

func applyOneDrain(ctx context.Context, db *sql.DB, pub ConfirmedPublisher, opts Options, msg Message, item *PlanItem, res *Result) error {
	if !opts.Apply {
		item.Status = StatusWouldDrain
		res.Items = append(res.Items, *item)
		return nil
	}

	if err := pub.PublishConfirmed(ctx, msg); err != nil {
		item.Status = StatusError
		item.Detail = err.Error()
		res.Errors++
		res.Items = append(res.Items, *item)
		res.Aborted = true
		res.AbortWhy = "publish confirm failed"
		return nil
	}

	if err := commitDrain(ctx, db, opts.Actor, msg); err != nil {
		// At-least-once: message was ACKed; audit/delete failed — report error.
		item.Status = StatusError
		item.Detail = "acked but audit/delete failed: " + err.Error()
		res.Errors++
		res.Items = append(res.Items, *item)
		res.Aborted = true
		res.AbortWhy = item.Detail
		return nil
	}
	item.Status = StatusDrained
	res.Drained++
	res.Items = append(res.Items, *item)
	return nil
}

type outboxRow struct {
	ID          string
	EventType   string
	AggregateID string
	EventBlob   string
}

type preparedRow struct {
	row  outboxRow
	msg  Message
	item PlanItem
}

func selectOutbox(ctx context.Context, db *sql.DB, ids []string) ([]outboxRow, error) {
	q := `SELECT id, event_type, aggregate_id, event_blob FROM outbox`
	var args []any
	if len(ids) > 0 {
		ph := make([]string, len(ids))
		for i, id := range ids {
			ph[i] = "?"
			args = append(args, id)
		}
		q += ` WHERE id IN (` + strings.Join(ph, ",") + `)`
	}
	q += ` ORDER BY available_at ASC, created_at ASC, id ASC`

	rs, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("drain: query outbox: %w", err)
	}
	defer rs.Close()

	var out []outboxRow
	for rs.Next() {
		var r outboxRow
		if err := rs.Scan(&r.ID, &r.EventType, &r.AggregateID, &r.EventBlob); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

func loadOutbox(ctx context.Context, db *sql.DB, id string) (outboxRow, bool, error) {
	var r outboxRow
	err := db.QueryRowContext(ctx,
		`SELECT id, event_type, aggregate_id, event_blob FROM outbox WHERE id = ?`, id,
	).Scan(&r.ID, &r.EventType, &r.AggregateID, &r.EventBlob)
	if errors.Is(err, sql.ErrNoRows) {
		return outboxRow{}, false, nil
	}
	if err != nil {
		return outboxRow{}, false, err
	}
	return r, true, nil
}

func decodeLegacyMessage(row outboxRow) (Message, error) {
	codec := &ehcodec.EventCodec{}
	event, _, err := codec.UnmarshalEvent(context.Background(), []byte(row.EventBlob))
	if err != nil {
		return Message{}, fmt.Errorf("outbox %s: unmarshal event: %w", row.ID, err)
	}
	meta := event.Metadata()
	if meta == nil {
		return Message{}, fmt.Errorf("outbox %s: %w", row.ID, errUnsupportedNative)
	}
	raw, ok := meta[metaBodyBase64]
	if !ok {
		return Message{}, fmt.Errorf("outbox %s: %w", row.ID, errUnsupportedNative)
	}
	encoded, ok := raw.(string)
	if !ok || encoded == "" {
		return Message{}, fmt.Errorf("outbox %s: invalid legacy.body_base64", row.ID)
	}
	body, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return Message{}, fmt.Errorf("outbox %s: decode body: %w", row.ID, err)
	}

	var routing []string
	if rk, ok := meta[metaRoutingKey].(string); ok && rk != "" {
		routing = append(routing, rk)
	}
	switch v := meta[metaRoutingKeys].(type) {
	case []string:
		routing = append(routing, v...)
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok && s != "" {
				routing = append(routing, s)
			}
		}
	}
	// Dedup preserve order.
	routing = uniqStrings(routing)
	if len(routing) == 0 {
		return Message{}, fmt.Errorf("outbox %s: missing legacy routing key(s)", row.ID)
	}

	headers := extractSafeHeaders(meta)

	return Message{
		OutboxID:    row.ID,
		EventType:   row.EventType,
		Body:        body,
		RoutingKeys: routing,
		Headers:     headers,
	}, nil
}

// extractSafeHeaders builds the AMQP header map for republish.
//   - Envelope fields (correlation/message id/type/origin) are kept under their
//     legacy.* keys for publisher property mapping (not secrets).
//   - Application headers stored as legacy.header.<name> are restored under the
//     original AMQP name only when present in safeDrainHeaderNames.
func extractSafeHeaders(meta map[string]any) map[string]any {
	headers := map[string]any{}
	for _, key := range []string{metaCorrelationID, metaCausationID, metaMessageID, metaOrigin, metaLegacyType} {
		if v, ok := meta[key]; ok {
			headers[key] = v
		}
	}
	for k, v := range meta {
		if !strings.HasPrefix(k, metaHeaderPrefix) {
			continue
		}
		name := strings.TrimPrefix(k, metaHeaderPrefix)
		if name == "" {
			continue
		}
		if _, ok := safeDrainHeaderNames[name]; !ok {
			continue
		}
		headers[name] = v
	}
	return headers
}

var errUnsupportedNative = errors.New("native EH event without legacy.body_base64 (abort; not drained)")

func auditTableExists(db *sql.DB) (bool, error) {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='outbox_drain_audit'`,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func isDrained(ctx context.Context, db *sql.DB, outboxID string) (bool, error) {
	// Dry-run / old DB without audit table: treat as never drained (compatible fallback).
	ok, err := auditTableExists(db)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	var n int
	err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_drain_audit WHERE outbox_id = ?`, outboxID).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func commitDrain(ctx context.Context, db *sql.DB, actor string, msg Message) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rk, _ := json.Marshal(msg.RoutingKeys)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_drain_audit (outbox_id, drained_at, drained_by, event_type, routing_keys)
		VALUES (?, ?, ?, ?, ?)
	`, msg.OutboxID, time.Now(), actor, msg.EventType, string(rk)); err != nil {
		// Concurrent drain: primary-key conflict means another worker already
		// audited; still ensure the outbox row is gone. Classify via typed
		// sqlite3.Error — never by matching error text.
		if !isSQLiteConstraintConflict(err) {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM outbox WHERE id = ?`, msg.OutboxID); err != nil {
		return err
	}
	return tx.Commit()
}

// isSQLiteConstraintConflict reports concurrent audit insert races only:
// UNIQUE or PRIMARY KEY violations. Other constraint codes (CHECK, NOT NULL,
// FK, …) must NOT be treated as success — otherwise commitDrain would DELETE
// the outbox row after a failed audit write.
func isSQLiteConstraintConflict(err error) bool {
	var se sqlite3.Error
	if !errors.As(err, &se) {
		return false
	}
	switch se.ExtendedCode {
	case sqlite3.ErrConstraintUnique, sqlite3.ErrConstraintPrimaryKey:
		return true
	default:
		return false
	}
}

func uniqStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// dedupeIDsPreserveOrder drops empty/duplicate IDs, keeping first-seen order.
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
