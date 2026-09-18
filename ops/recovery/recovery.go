// Package recovery provides the policy layer for offline outbox recovery.
package recovery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vercly/eh-sqlite/schema"
)

// RemapOptions describes an explicit, offline recipient remap. Apply remains
// false by default so callers can always inspect the planned operation first.
type RemapOptions struct {
	Apply            bool
	Actor            string
	DeliveryIDs      []string
	DeadLetterIDs    []string
	Mapping          map[string]string
	ProcessorStopped bool
	BackupConfirmed  bool
}

// UnclaimedDelivery is an unresolved durable delivery. Handler identities are
// deliberately opaque: only an explicit operator mapping may change them.
type UnclaimedDelivery struct {
	ID             string    `json:"id"`
	PublicationID  string    `json:"publication_id"`
	LegacyOutboxID string    `json:"legacy_outbox_id,omitempty"`
	HandlerType    *string   `json:"handler_type,omitempty"`
	EventType      string    `json:"event_type"`
	AggregateID    string    `json:"aggregate_id"`
	CreatedAt      time.Time `json:"created_at"`
	AvailableAt    time.Time `json:"available_at"`
	UnresolvedAt   time.Time `json:"unresolved_at"`
}

// RemapItem records the planned or committed change for a single delivery.
type RemapItem struct {
	DeliveryID    string `json:"delivery_id"`
	PublicationID string `json:"publication_id,omitempty"`
	OldRecipient  string `json:"old_recipient,omitempty"`
	NewRecipient  string `json:"new_recipient,omitempty"`
	Status        string `json:"status"`
	Detail        string `json:"detail,omitempty"`
}

// RemapResult contains a dry-run plan or the audit-backed mutation result.
type RemapResult struct {
	DryRun  bool        `json:"dry_run"`
	Actor   string      `json:"actor,omitempty"`
	Items   []RemapItem `json:"items"`
	Applied int         `json:"applied"`
	Missing int         `json:"missing"`
	Errors  int         `json:"errors"`
}

// ValidateRemapOptions validates the irreversible parts of a remap before a
// storage implementation opens a write transaction. It deliberately does not
// infer old listener identities or accept a broad apply.
func ValidateRemapOptions(opts RemapOptions) (RemapOptions, error) {
	opts.DeliveryIDs = dedupe(opts.DeliveryIDs)
	opts.DeadLetterIDs = dedupe(opts.DeadLetterIDs)
	if len(opts.Mapping) == 0 {
		return opts, errors.New("recovery: an explicit recipient mapping is required")
	}
	for oldID, newID := range opts.Mapping {
		if strings.TrimSpace(oldID) == "" || strings.TrimSpace(newID) == "" {
			return opts, fmt.Errorf("recovery: mapping entries must have non-empty old and new recipient ids")
		}
		if strings.TrimSpace(oldID) == strings.TrimSpace(newID) {
			return opts, fmt.Errorf("recovery: mapping %q does not change the recipient", oldID)
		}
	}
	if !opts.Apply {
		return opts, nil
	}
	if strings.TrimSpace(opts.Actor) == "" {
		return opts, errors.New("recovery: --actor is required with --apply")
	}
	if len(opts.DeliveryIDs) == 0 && len(opts.DeadLetterIDs) == 0 {
		return opts, errors.New("recovery: --apply requires explicit delivery or dead-letter ids")
	}
	if !opts.ProcessorStopped {
		return opts, errors.New("recovery: --apply requires confirmation that the outbox processor is stopped")
	}
	if !opts.BackupConfirmed {
		return opts, errors.New("recovery: --apply requires confirmation of a database backup")
	}
	return opts, nil
}

// ValidateV2Schema is read-only and rejects a v1 database instead of trying
// to operate on handlers JSON. The service migration owns the schema change.
func ValidateV2Schema(ctx context.Context, db *sql.DB) error {
	applied, err := schema.IsApplied(ctx, db, "outbox", "v2_deliveries")
	if err != nil {
		return fmt.Errorf("recovery: check outbox migration: %w", err)
	}
	if !applied {
		return errors.New("recovery: outbox v2 migration is pending; start the service once before using delivery recovery")
	}
	for _, table := range []string{"outbox_publications", "outbox_deliveries"} {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&n); err != nil {
			return fmt.Errorf("recovery: check %s: %w", table, err)
		}
		if n == 0 {
			return fmt.Errorf("recovery: required table %q is missing", table)
		}
	}
	return nil
}

// ListUnclaimed returns the durable unresolved signal stamped by StartChecked.
// It includes failed rematch sentinels, whose handler_type is NULL.
func ListUnclaimed(ctx context.Context, db *sql.DB) ([]UnclaimedDelivery, error) {
	if err := ValidateV2Schema(ctx, db); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT d.id, d.publication_id, d.legacy_outbox_id, d.handler_type,
		       d.event_type, d.aggregate_id, d.created_at, d.available_at, d.unresolved_at
		FROM outbox_deliveries d
		WHERE d.unresolved_at IS NOT NULL
		ORDER BY d.unresolved_at ASC, d.seq ASC`)
	if err != nil {
		return nil, fmt.Errorf("recovery: list unclaimed: %w", err)
	}
	defer rows.Close()
	var result []UnclaimedDelivery
	for rows.Next() {
		var item UnclaimedDelivery
		var handler, legacyOutboxID sql.NullString
		if err := rows.Scan(&item.ID, &item.PublicationID, &legacyOutboxID, &handler,
			&item.EventType, &item.AggregateID, &item.CreatedAt, &item.AvailableAt, &item.UnresolvedAt); err != nil {
			return nil, fmt.Errorf("recovery: scan unclaimed: %w", err)
		}
		if legacyOutboxID.Valid {
			item.LegacyOutboxID = legacyOutboxID.String
		}
		if handler.Valid {
			item.HandlerType = &handler.String
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// RemapRecipients plans or atomically applies an explicit recipient identity
// mapping to selected unresolved deliveries. It never clears handler_type to
// force a rematch; sentinels and unselected rows are rejected instead.
func RemapRecipients(ctx context.Context, db *sql.DB, opts RemapOptions) (RemapResult, error) {
	var err error
	opts, err = ValidateRemapOptions(opts)
	if err != nil {
		return RemapResult{}, err
	}
	if err := ValidateV2Schema(ctx, db); err != nil {
		return RemapResult{}, err
	}
	result := RemapResult{DryRun: !opts.Apply, Actor: opts.Actor}
	items, err := loadSelected(ctx, db, opts.DeliveryIDs)
	if err != nil {
		return result, err
	}
	byID := make(map[string]selectedDelivery, len(items))
	for _, item := range items {
		byID[item.ID] = item
	}
	plannedRecipients := make(map[string]string, len(opts.DeliveryIDs))
	for _, id := range opts.DeliveryIDs {
		item, ok := byID[id]
		if !ok {
			result.Items = append(result.Items, RemapItem{DeliveryID: id, Status: "missing", Detail: "delivery id not found"})
			result.Missing++
			result.Errors++
			continue
		}
		plan := RemapItem{DeliveryID: item.ID, PublicationID: item.PublicationID, Status: "would_remap"}
		if !item.UnresolvedAt.Valid || item.TakenAt.Valid {
			plan.Status, plan.Detail = "error", "delivery is not an unclaimed unresolved recipient"
			result.Errors++
			result.Items = append(result.Items, plan)
			continue
		}
		if !item.HandlerType.Valid {
			plan.Status, plan.Detail = "error", "rematch sentinel has no recipient identity to remap"
			result.Errors++
			result.Items = append(result.Items, plan)
			continue
		}
		plan.OldRecipient = item.HandlerType.String
		newRecipient, ok := opts.Mapping[item.HandlerType.String]
		if !ok {
			plan.Status, plan.Detail = "error", "no explicit mapping for selected recipient"
			result.Errors++
			result.Items = append(result.Items, plan)
			continue
		}
		plan.NewRecipient = strings.TrimSpace(newRecipient)
		planKey := item.PublicationID + "\x00" + plan.NewRecipient
		if otherID, duplicate := plannedRecipients[planKey]; duplicate {
			plan.Status, plan.Detail = "error", fmt.Sprintf("recipient is also selected by delivery %s in this publication", otherID)
			result.Errors++
			result.Items = append(result.Items, plan)
			continue
		}
		plannedRecipients[planKey] = item.ID
		collision, err := recipientCollision(ctx, db, item.ID, item.PublicationID, plan.NewRecipient)
		if err != nil {
			return result, err
		}
		if collision {
			plan.Status, plan.Detail = "error", "recipient already has a remaining delivery in this publication"
			result.Errors++
		}
		result.Items = append(result.Items, plan)
	}
	if !opts.Apply || result.Errors > 0 {
		return result, nil
	}
	if err := ensureAuditSchema(db); err != nil {
		return result, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	now := schema.UTC(time.Now())
	applied := make([]int, 0, len(result.Items))
	for i := range result.Items {
		item := &result.Items[i]
		if item.Status != "would_remap" {
			continue
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE outbox_deliveries
			SET handler_type = ?, unresolved_at = NULL, dispatch_key = NULL, dispatch_config = NULL
			WHERE id = ? AND handler_type = ? AND unresolved_at IS NOT NULL`,
			item.NewRecipient, item.DeliveryID, item.OldRecipient)
		if err != nil {
			return result, fmt.Errorf("recovery: remap delivery %s: %w", item.DeliveryID, err)
		}
		affected, _ := res.RowsAffected()
		if affected != 1 {
			return result, fmt.Errorf("recovery: delivery %s changed concurrently or is no longer unresolved", item.DeliveryID)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO outbox_recipient_remap_audit
			(delivery_id, publication_id, old_recipient, new_recipient, remapped_at, remapped_by)
			VALUES (?, ?, ?, ?, ?, ?)`,
			item.DeliveryID, item.PublicationID, item.OldRecipient, item.NewRecipient, now, opts.Actor); err != nil {
			return result, fmt.Errorf("recovery: audit delivery %s: %w", item.DeliveryID, err)
		}
		applied = append(applied, i)
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("recovery: commit remap: %w", err)
	}
	for _, i := range applied {
		result.Items[i].Status = "remapped"
	}
	result.Applied = len(applied)
	return result, nil
}

// RemapDeadLetterRecipients handles historical per-handler DLQ rows explicitly.
// It changes only selected source=outbox rows and preserves the payload and
// provenance columns for later replay; it never clears handler_type.
func RemapDeadLetterRecipients(ctx context.Context, db *sql.DB, opts RemapOptions) (RemapResult, error) {
	var err error
	opts, err = ValidateRemapOptions(opts)
	if err != nil {
		return RemapResult{}, err
	}
	if err := ValidateV2Schema(ctx, db); err != nil {
		return RemapResult{}, err
	}
	if len(opts.DeadLetterIDs) == 0 {
		return RemapResult{}, errors.New("recovery: explicit dead-letter ids are required")
	}
	result := RemapResult{DryRun: !opts.Apply, Actor: opts.Actor}
	rows, err := loadDeadLetters(ctx, db, opts.DeadLetterIDs)
	if err != nil {
		return result, err
	}
	byID := make(map[string]deadLetter, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	for _, id := range opts.DeadLetterIDs {
		row, ok := byID[id]
		if !ok {
			result.Items = append(result.Items, RemapItem{DeliveryID: id, Status: "missing", Detail: "outbox dead-letter id not found"})
			result.Missing++
			result.Errors++
			continue
		}
		newRecipient, ok := opts.Mapping[row.HandlerType]
		item := RemapItem{DeliveryID: row.ID, PublicationID: row.PublicationID, OldRecipient: row.HandlerType, Status: "would_remap"}
		if !ok {
			item.Status, item.Detail = "error", "no explicit mapping for selected recipient"
			result.Errors++
		} else {
			item.NewRecipient = strings.TrimSpace(newRecipient)
		}
		result.Items = append(result.Items, item)
	}
	if !opts.Apply || result.Errors > 0 {
		return result, nil
	}
	if err := ensureAuditSchema(db); err != nil {
		return result, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() { _ = tx.Rollback() }()
	now := schema.UTC(time.Now())
	for i := range result.Items {
		item := &result.Items[i]
		res, err := tx.ExecContext(ctx, `UPDATE dead_letters SET handler_type = ? WHERE id = ? AND source = 'outbox' AND handler_type = ?`, item.NewRecipient, item.DeliveryID, item.OldRecipient)
		if err != nil {
			return result, fmt.Errorf("recovery: remap dead letter %s: %w", item.DeliveryID, err)
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return result, fmt.Errorf("recovery: dead letter %s changed concurrently", item.DeliveryID)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_recipient_remap_audit (record_kind, delivery_id, publication_id, old_recipient, new_recipient, remapped_at, remapped_by) VALUES ('dead_letter', ?, ?, ?, ?, ?, ?)`, item.DeliveryID, item.PublicationID, item.OldRecipient, item.NewRecipient, now, opts.Actor); err != nil {
			return result, fmt.Errorf("recovery: audit dead letter %s: %w", item.DeliveryID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("recovery: commit dead-letter remap: %w", err)
	}
	for i := range result.Items {
		result.Items[i].Status = "remapped"
	}
	result.Applied = len(result.Items)
	return result, nil
}

type deadLetter struct{ ID, HandlerType, PublicationID string }

func loadDeadLetters(ctx context.Context, db *sql.DB, ids []string) ([]deadLetter, error) {
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i], args[i] = "?", id
	}
	rows, err := db.QueryContext(ctx, `SELECT id, handler_type, COALESCE(publication_id, '') FROM dead_letters WHERE source = 'outbox' AND id IN (`+strings.Join(ph, ",")+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("recovery: select dead letters: %w", err)
	}
	defer rows.Close()
	var result []deadLetter
	for rows.Next() {
		var row deadLetter
		if err := rows.Scan(&row.ID, &row.HandlerType, &row.PublicationID); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

type selectedDelivery struct {
	ID            string
	PublicationID string
	HandlerType   sql.NullString
	UnresolvedAt  sql.NullTime
	TakenAt       sql.NullTime
}

func loadSelected(ctx context.Context, db *sql.DB, ids []string) ([]selectedDelivery, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i], args[i] = "?", id
	}
	rows, err := db.QueryContext(ctx, `SELECT id, publication_id, handler_type, unresolved_at, taken_at FROM outbox_deliveries WHERE id IN (`+strings.Join(placeholders, ",")+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("recovery: select deliveries: %w", err)
	}
	defer rows.Close()
	var result []selectedDelivery
	for rows.Next() {
		var item selectedDelivery
		if err := rows.Scan(&item.ID, &item.PublicationID, &item.HandlerType, &item.UnresolvedAt, &item.TakenAt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func recipientCollision(ctx context.Context, db *sql.DB, deliveryID, publicationID, recipient string) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox_deliveries WHERE publication_id = ? AND handler_type = ? AND id <> ?`, publicationID, recipient, deliveryID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("recovery: check recipient collision: %w", err)
	}
	return n > 0, nil
}

func ensureAuditSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox_recipient_remap_audit (
			record_kind TEXT NOT NULL DEFAULT 'delivery',
			delivery_id TEXT NOT NULL,
			publication_id TEXT NOT NULL,
			old_recipient TEXT NOT NULL,
			new_recipient TEXT NOT NULL,
			remapped_at TIMESTAMP NOT NULL,
			remapped_by TEXT NOT NULL,
			PRIMARY KEY (delivery_id, remapped_at)
		)`)
	if err != nil {
		return fmt.Errorf("recovery: ensure remap audit table: %w", err)
	}
	return nil
}

func dedupe(ids []string) []string {
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
