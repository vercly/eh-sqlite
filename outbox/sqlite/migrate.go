package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/vercly/eh-sqlite/internal/deadletter"
	"github.com/vercly/eh-sqlite/schema"
	"github.com/vercly/eventhorizon/uuid"
)

// MigrationReport describes what Migrate did.
type MigrationReport struct {
	// Applied is true when the v1 → v2 migration ran in this call. False means
	// the marker already existed (nothing was touched).
	Applied bool
	// V1Table is the name of the v1 table that was migrated and renamed, or ""
	// when no v1 table existed.
	V1Table string
	// Publications, Deliveries and Sentinels count rows created from v1 rows.
	Publications int64
	Deliveries   int64
	Sentinels    int64
}

// Migrate brings the outbox tables for prefix to the v2 layout:
//
//  1. creates eh_sqlite_migrations, the publication/delivery tables and the
//     dead_letters additive columns (all idempotent);
//  2. if the v1 table <prefix> exists and the marker outbox/v2_deliveries
//     (per prefix) is absent, copies every v1 row into one publication plus
//     one delivery per remaining handler (or one rematch sentinel for
//     handlers=[]), converting timestamps to canonical UTC, preserving the v1
//     order (available_at, created_at, id) through the delivery sequence, then
//     renames the v1 table to <prefix>_v1_migrated;
//  3. records the marker in the same transaction.
//
// The v1 table is never read again by the library; it is an explicit backup
// with full payloads, retained until an operator drops it. Migrate is what
// StartChecked runs before dispatch; operator tools may call it explicitly
// with the processor stopped.
func Migrate(ctx context.Context, db *sql.DB, prefix string) (MigrationReport, error) {
	report := MigrationReport{}
	if err := schema.ValidateIdent(prefix); err != nil {
		return report, err
	}
	if err := ensureV2Tables(ctx, db, prefix); err != nil {
		return report, err
	}
	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		return report, err
	}

	applied, err := schema.Apply(ctx, db, migrationComponent, migrationNameFor(prefix), func(tx *sql.Tx) error {
		exists, err := schema.TableExistsTx(ctx, tx, prefix)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		report.V1Table = prefix
		rows, err := loadV1Rows(ctx, tx, prefix)
		if err != nil {
			return err
		}
		for _, row := range rows {
			// Routing envelope only: offline migration must not need the
			// application's event-data factories. An undecodable blob is
			// migrated verbatim; claim flags the delivery as unresolved
			// (visible), never deletes it.
			partitionKey, err := StoredEventPartitionKey([]byte(row.eventBlob))
			if err != nil {
				log.Printf("eventhorizon: outbox migration: could not read v1 row %s envelope, migrating without partition key: %v", row.id, err)
				partitionKey = ""
			}
			publicationID := uuid.New().String()
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
				INSERT INTO %s (publication_id, event_type, aggregate_id, partition_key, event_blob, created_at, origin, origin_ref)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, publicationsTableFor(prefix)),
				publicationID, row.eventType, row.aggregateID, partitionKey, row.eventBlob,
				row.createdAt, originMigrated, row.id); err != nil {
				return fmt.Errorf("could not migrate v1 row %s publication: %w", row.id, err)
			}
			report.Publications++
			if len(row.handlers) == 0 {
				if err := insertDelivery(ctx, tx, prefix, deliveryInsert{
					id:             uuid.New().String(),
					publicationID:  publicationID,
					handlerType:    sql.NullString{},
					eventType:      row.eventType,
					aggregateID:    row.aggregateID,
					createdAt:      row.createdAt,
					availableAt:    row.availableAt,
					retryCount:     row.retryCount,
					legacyOutboxID: sql.NullString{String: row.id, Valid: true},
				}); err != nil {
					return err
				}
				report.Sentinels++
				continue
			}
			for _, handlerType := range row.handlers {
				if err := insertDelivery(ctx, tx, prefix, deliveryInsert{
					id:             uuid.New().String(),
					publicationID:  publicationID,
					handlerType:    sql.NullString{String: handlerType, Valid: true},
					eventType:      row.eventType,
					aggregateID:    row.aggregateID,
					createdAt:      row.createdAt,
					availableAt:    row.availableAt,
					retryCount:     row.retryCount,
					legacyOutboxID: sql.NullString{String: row.id, Valid: true},
				}); err != nil {
					return err
				}
				report.Deliveries++
			}
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, prefix, v1MigratedTableFor(prefix))); err != nil {
			return fmt.Errorf("could not archive v1 outbox table: %w", err)
		}
		return nil
	})
	if err != nil {
		return MigrationReport{}, err
	}
	report.Applied = applied
	if !applied {
		report.V1Table = ""
	}
	return report, nil
}

// ensureV2Tables creates the bookkeeping and v2 tables outside of any marker.
func ensureV2Tables(ctx context.Context, db *sql.DB, prefix string) error {
	if err := schema.EnsureMigrationsTable(db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin outbox schema transaction: %w", err)
	}
	defer rollbackTx(tx)
	if err := createV2Schema(ctx, tx, prefix); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit outbox schema: %w", err)
	}
	return nil
}

type v1Row struct {
	id          string
	eventType   string
	aggregateID string
	createdAt   time.Time
	availableAt time.Time
	handlers    []string
	eventBlob   string
	retryCount  int
}

// loadV1Rows reads every v1 row, normalizes its timestamps (fail closed on
// ambiguous values) and returns them in the v1 selection order
// (available_at, created_at, id) computed on parsed instants, not on text.
func loadV1Rows(ctx context.Context, tx *sql.Tx, table string) ([]v1Row, error) {
	hasAvailable, err := schema.ColumnExistsTx(ctx, tx, table, "available_at")
	if err != nil {
		return nil, err
	}
	hasRetry, err := schema.ColumnExistsTx(ctx, tx, table, "retry_count")
	if err != nil {
		return nil, err
	}
	availableExpr := "NULL"
	if hasAvailable {
		availableExpr = "CAST(available_at AS TEXT)"
	}
	retryExpr := "0"
	if hasRetry {
		retryExpr = "COALESCE(retry_count, 0)"
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT id, event_type, aggregate_id, CAST(created_at AS TEXT), %s, handlers, event_blob, %s
		FROM %s`, availableExpr, retryExpr, table))
	if err != nil {
		return nil, fmt.Errorf("could not read v1 outbox rows: %w", err)
	}
	defer rows.Close()

	var out []v1Row
	for rows.Next() {
		var (
			r            v1Row
			createdRaw   string
			availableRaw sql.NullString
			handlersBlob string
		)
		if err := rows.Scan(&r.id, &r.eventType, &r.aggregateID, &createdRaw, &availableRaw, &handlersBlob, &r.eventBlob, &r.retryCount); err != nil {
			return nil, fmt.Errorf("could not scan v1 outbox row: %w", err)
		}
		createdAt, err := schema.ParseStored(createdRaw)
		if err != nil {
			return nil, fmt.Errorf("%w (table %s, column created_at, id %s)", err, table, r.id)
		}
		r.createdAt = createdAt
		r.availableAt = createdAt
		if availableRaw.Valid && availableRaw.String != "" {
			availableAt, err := schema.ParseStored(availableRaw.String)
			if err != nil {
				return nil, fmt.Errorf("%w (table %s, column available_at, id %s)", err, table, r.id)
			}
			r.availableAt = availableAt
		}
		if err := jsoniter.Unmarshal([]byte(handlersBlob), &r.handlers); err != nil {
			return nil, fmt.Errorf("could not parse v1 outbox row %s handlers: %w", r.id, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if !a.availableAt.Equal(b.availableAt) {
			return a.availableAt.Before(b.availableAt)
		}
		if !a.createdAt.Equal(b.createdAt) {
			return a.createdAt.Before(b.createdAt)
		}
		return a.id < b.id
	})
	return out, nil
}

type deliveryInsert struct {
	id             string
	publicationID  string
	handlerType    sql.NullString // invalid = rematch sentinel
	dispatchKey    sql.NullString
	dispatchConfig sql.NullString
	eventType      string
	aggregateID    string
	createdAt      time.Time
	availableAt    time.Time
	retryCount     int
	legacyOutboxID sql.NullString
}

func insertDelivery(ctx context.Context, tx *sql.Tx, prefix string, d deliveryInsert) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, publication_id, handler_type, dispatch_key, dispatch_config, event_type, aggregate_id,
		                created_at, available_at, taken_at, retry_count, unresolved_at, legacy_outbox_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, ?)`, deliveriesTableFor(prefix)),
		d.id, d.publicationID, d.handlerType, d.dispatchKey, d.dispatchConfig, d.eventType, d.aggregateID,
		schema.UTC(d.createdAt), schema.UTC(d.availableAt), d.retryCount, d.legacyOutboxID); err != nil {
		return fmt.Errorf("could not insert outbox delivery %s: %w", d.id, err)
	}
	return nil
}

// errV1TableStillPresent guards against a half-migrated state that should be
// impossible (marker without rename) but is reported instead of ignored.
var errV1TableStillPresent = errors.New("outbox: v1 table present although v2 migration marker exists")

// verifyMigrated reports errV1TableStillPresent when the marker exists but the
// v1 table was not archived.
func verifyMigrated(ctx context.Context, db *sql.DB, prefix string) error {
	applied, err := schema.IsApplied(ctx, db, migrationComponent, migrationNameFor(prefix))
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}
	exists, err := schema.TableExists(ctx, db, prefix)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("%w: %s", errV1TableStillPresent, prefix)
	}
	return nil
}
