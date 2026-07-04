package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vercly/eh-sqlite/internal/deadletter"
	outboxsqlite "github.com/vercly/eh-sqlite/outbox/sqlite"
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
		`, now.Add(-cfg.asyncCompletedRetention))
		if err != nil {
			return result, fmt.Errorf("cleanup completed async_tasks: %w", err)
		}
		result.AsyncCompletedDeleted = deleted

		deleted, err = deleteRows(ctx, db, `
			DELETE FROM async_tasks
			WHERE status = 'failed_permanent' AND updated_at < ?
		`, now.Add(-cfg.failedRetention))
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
	`, now.Add(-cfg.deadLetterRetention))
	if err != nil {
		return result, fmt.Errorf("cleanup dead_letters: %w", err)
	}
	result.DeadLettersDeleted = deleted

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
	PendingRows        int64
	AvailableRows      int64
	RetryRows          int64
	RetryCountTotal    int64
	OldestPendingAge   time.Duration
	OldestAvailableAge time.Duration
	DueLag             time.Duration
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

	if exists, err := tableExists(ctx, db, "outbox"); err != nil {
		return snapshot, err
	} else if exists {
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
	err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN available_at <= ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(retry_count), 0)
		FROM outbox
	`, now).Scan(&snapshot.PendingRows, &snapshot.AvailableRows, &snapshot.RetryRows, &snapshot.RetryCountTotal)
	if err != nil {
		return snapshot, fmt.Errorf("query outbox counts: %w", err)
	}

	oldestPending, err := queryMinTime(ctx, db, `SELECT MIN(created_at) FROM outbox`)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest pending outbox row: %w", err)
	}
	oldestAvailable, err := queryMinTime(ctx, db, `SELECT MIN(created_at) FROM outbox WHERE available_at <= ?`, now)
	if err != nil {
		return snapshot, fmt.Errorf("query oldest available outbox row: %w", err)
	}
	oldestDue, err := queryMinTime(ctx, db, `SELECT MIN(available_at) FROM outbox WHERE available_at <= ?`, now)
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

func parseSQLiteTime(value string) (sql.NullTime, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return sql.NullTime{}, nil
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return sql.NullTime{Time: parsed, Valid: true}, nil
		}
	}
	return sql.NullTime{}, fmt.Errorf("parse SQLite time %q", value)
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
