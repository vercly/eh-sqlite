package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vercly/eh-sqlite/internal/deadletter"
	outboxsqlite "github.com/vercly/eh-sqlite/outbox/sqlite"
	"github.com/vercly/eh-sqlite/schema"
)

const (
	defaultCleanupInterval         = time.Hour
	defaultAsyncCompletedRetention = 36 * time.Hour
	defaultFailedRetention         = 30 * 24 * time.Hour
	defaultDeadLetterRetention     = 30 * 24 * time.Hour
	defaultStatsInterval           = time.Minute
)

// Result reports how many rows were removed by a cleanup pass.
type Result struct {
	AsyncCompletedDeleted       int64
	AsyncFailedPermanentDeleted int64
	DeadLettersDeleted          int64
	OrphanPublicationsDeleted   int64
}

// CleanupRun is emitted by StartCleanup after each cleanup pass.
type CleanupRun struct {
	Result Result
	Err    error
}

type options struct {
	cleanupInterval         time.Duration
	asyncCompletedRetention time.Duration
	failedRetention         time.Duration
	deadLetterRetention     time.Duration
	now                     func() time.Time
}

// Option configures cleanup retention and scheduling.
type Option func(*options)

// WithCleanupInterval sets how often StartCleanup runs Cleanup.
func WithCleanupInterval(interval time.Duration) Option {
	return func(o *options) {
		if interval > 0 {
			o.cleanupInterval = interval
		}
	}
}

// WithAsyncCompletedRetention sets retention for completed async_tasks rows.
func WithAsyncCompletedRetention(retention time.Duration) Option {
	return func(o *options) {
		if retention > 0 {
			o.asyncCompletedRetention = retention
		}
	}
}

// WithFailedRetention sets retention for failed_permanent async_tasks rows.
func WithFailedRetention(retention time.Duration) Option {
	return func(o *options) {
		if retention > 0 {
			o.failedRetention = retention
		}
	}
}

// WithDeadLetterRetention sets retention for exported dead_letters rows.
func WithDeadLetterRetention(retention time.Duration) Option {
	return func(o *options) {
		if retention > 0 {
			o.deadLetterRetention = retention
		}
	}
}

// WithNow overrides the clock used by cleanup and stats. It is mainly useful
// for deterministic tests.
func WithNow(now func() time.Time) Option {
	return func(o *options) {
		if now != nil {
			o.now = now
		}
	}
}

func defaultOptions() options {
	return options{
		cleanupInterval:         defaultCleanupInterval,
		asyncCompletedRetention: defaultAsyncCompletedRetention,
		failedRetention:         defaultFailedRetention,
		deadLetterRetention:     defaultDeadLetterRetention,
		now:                     time.Now,
	}
}

func applyOptions(opts ...Option) options {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Cleanup deletes retained async task and dead-letter rows. Dead letters are
// deleted only after a successful export has set exported_at.
func Cleanup(ctx context.Context, db *sql.DB, opts ...Option) (Result, error) {
	cfg := applyOptions(opts...)
	now := cfg.now()
	result := Result{}

	if exists, err := tableExists(ctx, db, "async_tasks"); err != nil {
		return result, err
	} else if exists {
		deleted, err := deleteRows(ctx, db, `
			DELETE FROM async_tasks
			WHERE status = 'completed' AND updated_at < ?
		`, schema.UTC(now.Add(-cfg.asyncCompletedRetention)))
		if err != nil {
			return result, fmt.Errorf("cleanup completed async_tasks: %w", err)
		}
		result.AsyncCompletedDeleted = deleted

		deleted, err = deleteRows(ctx, db, `
			DELETE FROM async_tasks
			WHERE status = 'failed_permanent' AND updated_at < ?
		`, schema.UTC(now.Add(-cfg.failedRetention)))
		if err != nil {
			return result, fmt.Errorf("cleanup failed async_tasks: %w", err)
		}
		result.AsyncFailedPermanentDeleted = deleted
	}

	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		return result, err
	}
	deleted, err := deleteRows(ctx, db, `
		DELETE FROM dead_letters
		WHERE exported_at IS NOT NULL AND exported_at < ?
	`, schema.UTC(now.Add(-cfg.deadLetterRetention)))
	if err != nil {
		return result, fmt.Errorf("cleanup dead_letters: %w", err)
	}
	result.DeadLettersDeleted = deleted

	publicationsExist, err := tableExists(ctx, db, "outbox_publications")
	if err != nil {
		return result, err
	}
	deliveriesExist, err := tableExists(ctx, db, "outbox_deliveries")
	if err != nil {
		return result, err
	}
	if publicationsExist != deliveriesExist {
		return result, fmt.Errorf("cleanup outbox v2: publications and deliveries tables must both exist")
	}
	if publicationsExist {
		deleted, err = deleteRows(ctx, db, `
			DELETE FROM outbox_publications
			WHERE NOT EXISTS (
				SELECT 1 FROM outbox_deliveries
				WHERE outbox_deliveries.publication_id = outbox_publications.publication_id
			)
		`)
		if err != nil {
			return result, fmt.Errorf("cleanup orphan outbox publications: %w", err)
		}
		result.OrphanPublicationsDeleted = deleted
	}

	return result, nil
}

// StartCleanup runs Cleanup on a ticker until ctx is cancelled. The first run
// happens immediately.
func StartCleanup(ctx context.Context, db *sql.DB, opts ...Option) <-chan CleanupRun {
	cfg := applyOptions(opts...)
	runs := make(chan CleanupRun, 1)
	go func() {
		defer close(runs)
		runCleanup := func() bool {
			result, err := Cleanup(ctx, db, opts...)
			select {
			case runs <- CleanupRun{Result: result, Err: err}:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !runCleanup() {
			return
		}
		ticker := time.NewTicker(cfg.cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !runCleanup() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return runs
}

// Snapshot is a point-in-time view of eh-sqlite transport health.
type Snapshot struct {
	CapturedAt    time.Time
	Outbox        OutboxSnapshot
	DeadLetters   DeadLetterSnapshot
	AsyncTasks    map[string]int64
	ErrorSeverity map[string]int64
}

// OutboxSnapshot contains queue depth and delay information.
type OutboxSnapshot struct {
	PendingRows           int64
	AvailableRows         int64
	RetryRows             int64
	RetryCountTotal       int64
	OldestPendingAge      time.Duration
	OldestAvailableAge    time.Duration
	DueLag                time.Duration
	DeliveryRows          int64
	DeliveryAvailableRows int64
	DeliveryInFlightRows  int64
	DeliveryUnresolved    int64
	DeliveryRematch       int64
	AdmissionWaitAge      time.Duration
}

// DeadLetterSnapshot contains dead-letter counts.
type DeadLetterSnapshot struct {
	Total    int64
	BySource map[string]int64
}

// Stats calculates a point-in-time snapshot from the database and process-local
// outbox error counters.
func Stats(ctx context.Context, db *sql.DB, opts ...Option) (Snapshot, error) {
	cfg := applyOptions(opts...)
	now := cfg.now()
	snapshot := Snapshot{
		CapturedAt:    now,
		AsyncTasks:    map[string]int64{},
		ErrorSeverity: errorCounts(),
		DeadLetters: DeadLetterSnapshot{
			BySource: map[string]int64{},
		},
	}

	publicationsExist, err := tableExists(ctx, db, "outbox_publications")
	if err != nil {
		return snapshot, err
	}
	deliveriesExist, err := tableExists(ctx, db, "outbox_deliveries")
	if err != nil {
		return snapshot, err
	}
	if publicationsExist != deliveriesExist {
		return snapshot, fmt.Errorf("outbox v2 publications and deliveries tables must both exist")
	}
	v1Exists, err := tableExists(ctx, db, "outbox")
	if err != nil {
		return snapshot, err
	}
	v2Applied, err := schema.IsApplied(ctx, db, "outbox", "v2_deliveries")
	if err != nil {
		return snapshot, err
	}
	// NewOutbox creates empty v2 tables before StartChecked performs the
	// migration. Keep reporting the durable v1 backlog until the transactional
	// migration marker proves that v2 owns the data.
	if v1Exists && !v2Applied {
		outbox, err := outboxStats(ctx, db, now)
		if err != nil {
			return snapshot, err
		}
		snapshot.Outbox = outbox
	} else if publicationsExist {
		outbox, err := outboxV2Stats(ctx, db, now)
		if err != nil {
			return snapshot, err
		}
		snapshot.Outbox = outbox
	} else if v1Exists {
		outbox, err := outboxStats(ctx, db, now)
		if err != nil {
			return snapshot, err
		}
		snapshot.Outbox = outbox
	}

	if exists, err := tableExists(ctx, db, "async_tasks"); err != nil {
		return snapshot, err
	} else if exists {
		statuses, err := asyncTaskStats(ctx, db)
		if err != nil {
			return snapshot, err
		}
		snapshot.AsyncTasks = statuses
	}

	if exists, err := tableExists(ctx, db, "dead_letters"); err != nil {
		return snapshot, err
	} else if exists {
		deadLetters, err := deadLetterStats(ctx, db)
		if err != nil {
			return snapshot, err
		}
		snapshot.DeadLetters = deadLetters
	}

	return snapshot, nil
}

func outboxV2Stats(ctx context.Context, db *sql.DB, now time.Time) (OutboxSnapshot, error) {
	var snapshot OutboxSnapshot
	utcNow := schema.UTC(now)
	err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(DISTINCT p.publication_id),
			COUNT(DISTINCT CASE WHEN d.taken_at IS NULL AND d.available_at <= ? THEN p.publication_id END),
			COUNT(DISTINCT CASE WHEN d.retry_count > 0 THEN p.publication_id END),
			COALESCE(SUM(d.retry_count), 0),
			COUNT(d.id),
			COALESCE(SUM(CASE WHEN d.taken_at IS NULL AND d.unresolved_at IS NULL AND d.dispatch_key IS NOT NULL AND d.available_at <= ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN d.taken_at IS NOT NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN d.unresolved_at IS NOT NULL THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN d.handler_type IS NULL THEN 1 ELSE 0 END), 0)
		FROM outbox_publications p
		JOIN outbox_deliveries d ON d.publication_id = p.publication_id
	`, utcNow, utcNow).Scan(
		&snapshot.PendingRows,
		&snapshot.AvailableRows,
		&snapshot.RetryRows,
		&snapshot.RetryCountTotal,
		&snapshot.DeliveryRows,
		&snapshot.DeliveryAvailableRows,
		&snapshot.DeliveryInFlightRows,
		&snapshot.DeliveryUnresolved,
		&snapshot.DeliveryRematch,
	)
	if err != nil {
		return snapshot, fmt.Errorf("query outbox v2 counts: %w", err)
	}

	oldestPending, err := queryMinTime(ctx, db, `SELECT MIN(created_at) FROM outbox_publications`)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest pending outbox publication: %w", err)
	}
	oldestAvailable, err := queryMinTime(ctx, db, `
		SELECT MIN(p.created_at)
		FROM outbox_publications p
		JOIN outbox_deliveries d ON d.publication_id = p.publication_id
		WHERE d.taken_at IS NULL AND d.available_at <= ?
	`, utcNow)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest available outbox publication: %w", err)
	}
	oldestDue, err := queryMinTime(ctx, db, `
		SELECT MIN(available_at) FROM outbox_deliveries
		WHERE taken_at IS NULL AND available_at <= ?
	`, utcNow)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest due outbox delivery: %w", err)
	}
	oldestAdmissionWait, err := queryMinTime(ctx, db, `
		SELECT MIN(available_at) FROM outbox_deliveries
		WHERE taken_at IS NULL AND unresolved_at IS NULL AND dispatch_key IS NOT NULL AND available_at <= ?
	`, utcNow)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest delivery waiting for admission: %w", err)
	}
	snapshot.OldestPendingAge = positiveDuration(now, oldestPending)
	snapshot.OldestAvailableAge = positiveDuration(now, oldestAvailable)
	snapshot.DueLag = positiveDuration(now, oldestDue)
	snapshot.AdmissionWaitAge = positiveDuration(now, oldestAdmissionWait)
	return snapshot, nil
}

// Reporter receives stats snapshots without tying the library to Prometheus.
type Reporter interface {
	ReportStats(context.Context, Snapshot) error
}

// ReporterFunc adapts a function to Reporter.
type ReporterFunc func(context.Context, Snapshot) error

func (f ReporterFunc) ReportStats(ctx context.Context, snapshot Snapshot) error {
	return f(ctx, snapshot)
}

// StartStatsReporter calculates and reports stats on a ticker until ctx is
// cancelled. The first report happens immediately.
func StartStatsReporter(ctx context.Context, db *sql.DB, reporter Reporter, interval time.Duration, opts ...Option) <-chan error {
	errs := make(chan error, 1)
	if interval <= 0 {
		interval = defaultStatsInterval
	}
	go func() {
		defer close(errs)
		report := func() bool {
			snapshot, err := Stats(ctx, db, opts...)
			if err == nil && reporter != nil {
				err = reporter.ReportStats(ctx, snapshot)
			}
			if err == nil {
				return true
			}
			select {
			case errs <- err:
				return true
			case <-ctx.Done():
				return false
			}
		}
		if !report() {
			return
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !report() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return errs
}

func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query table %s existence: %w", table, err)
	}
	return true, nil
}

func deleteRows(ctx context.Context, db *sql.DB, query string, args ...any) (int64, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func outboxStats(ctx context.Context, db *sql.DB, now time.Time) (OutboxSnapshot, error) {
	var snapshot OutboxSnapshot
	// Every SQL comparison parameter is UTC: stored text is canonical UTC, so
	// text comparison is only monotone against a UTC-bound parameter.
	utcNow := schema.UTC(now)
	err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN available_at <= ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(retry_count), 0)
		FROM outbox
	`, utcNow).Scan(&snapshot.PendingRows, &snapshot.AvailableRows, &snapshot.RetryRows, &snapshot.RetryCountTotal)
	if err != nil {
		return snapshot, fmt.Errorf("query outbox counts: %w", err)
	}

	oldestPending, err := queryMinTime(ctx, db, `SELECT MIN(created_at) FROM outbox`)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest pending outbox row: %w", err)
	}
	oldestAvailable, err := queryMinTime(ctx, db, `SELECT MIN(created_at) FROM outbox WHERE available_at <= ?`, utcNow)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest available outbox row: %w", err)
	}
	oldestDue, err := queryMinTime(ctx, db, `SELECT MIN(available_at) FROM outbox WHERE available_at <= ?`, utcNow)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest due outbox row: %w", err)
	}
	snapshot.OldestPendingAge = positiveDuration(now, oldestPending)
	snapshot.OldestAvailableAge = positiveDuration(now, oldestAvailable)
	snapshot.DueLag = positiveDuration(now, oldestDue)
	return snapshot, nil
}

func asyncTaskStats(ctx context.Context, db *sql.DB) (map[string]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT status, COUNT(*) FROM async_tasks GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("query async task statuses: %w", err)
	}
	defer rows.Close()
	statuses := map[string]int64{}
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan async task status: %w", err)
		}
		statuses[status] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate async task statuses: %w", err)
	}
	return statuses, nil
}

func deadLetterStats(ctx context.Context, db *sql.DB) (DeadLetterSnapshot, error) {
	snapshot := DeadLetterSnapshot{BySource: map[string]int64{}}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dead_letters`).Scan(&snapshot.Total); err != nil {
		return snapshot, fmt.Errorf("query dead letters total: %w", err)
	}
	rows, err := db.QueryContext(ctx, `SELECT source, COUNT(*) FROM dead_letters GROUP BY source`)
	if err != nil {
		return snapshot, fmt.Errorf("query dead letters by source: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var source string
		var count int64
		if err := rows.Scan(&source, &count); err != nil {
			return snapshot, fmt.Errorf("scan dead letter source: %w", err)
		}
		snapshot.BySource[source] = count
	}
	if err := rows.Err(); err != nil {
		return snapshot, fmt.Errorf("iterate dead letter sources: %w", err)
	}
	return snapshot, nil
}

func queryMinTime(ctx context.Context, db *sql.DB, query string, args ...any) (sql.NullTime, error) {
	var raw any
	if err := db.QueryRowContext(ctx, query, args...).Scan(&raw); err != nil {
		return sql.NullTime{}, err
	}
	return nullTimeFromSQLite(raw)
}

func nullTimeFromSQLite(raw any) (sql.NullTime, error) {
	switch value := raw.(type) {
	case nil:
		return sql.NullTime{}, nil
	case time.Time:
		return sql.NullTime{Time: value, Valid: true}, nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return sql.NullTime{}, fmt.Errorf("unsupported time value %T", raw)
	}
}

// parseSQLiteTime parses a stored timestamp through the shared schema
// contract so the accepted layout list lives in exactly one place.
func parseSQLiteTime(value string) (sql.NullTime, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return sql.NullTime{}, nil
	}
	parsed, err := schema.ParseStored(value)
	if err != nil {
		return sql.NullTime{}, fmt.Errorf("parse SQLite time %q: %w", value, err)
	}
	return sql.NullTime{Time: parsed, Valid: true}, nil
}

func positiveDuration(now time.Time, value sql.NullTime) time.Duration {
	if !value.Valid || value.Time.IsZero() || value.Time.After(now) {
		return 0
	}
	return now.Sub(value.Time)
}

func errorCounts() map[string]int64 {
	counts := outboxsqlite.ErrorSnapshot()
	return map[string]int64{
		"fatal":     counts.Fatal,
		"retryable": counts.Retryable,
		"unknown":   counts.Unknown,
	}
}
