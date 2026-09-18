// Copyright (c) 2025 - The Event Horizon authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package durable

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/vercly/eh-sqlite/backoff"
	dl "github.com/vercly/eh-sqlite/deadletter"
	"github.com/vercly/eh-sqlite/internal/deadletter"
	"github.com/vercly/eh-sqlite/schema"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

const (
	statusNew             = "new"
	statusProcessing      = "processing"
	statusCompleted       = "completed"
	statusFailedRetriable = "failed_retriable"
	statusFailedPermanent = "failed_permanent"
)

const defaultSweepInterval = 15 * time.Second
const defaultStuckTimeout = 5 * time.Minute

// ErrorSeverity defines how a durable command error should be treated.
type ErrorSeverity int

const (
	SeverityUnknown ErrorSeverity = iota
	SeverityFatal
	SeverityRetryable
)

// CategorizedError can be implemented by command handlers to override the
// default retry behaviour. By default, handler errors are retryable.
type CategorizedError interface {
	error
	DurableSeverity() ErrorSeverity
}

type taskCompletionFuncKey struct{}
type taskIDContextKey struct{}

// TaskCompletionFunc is a function that signals the completion status of a task.
type TaskCompletionFunc func(status string, execErr error)

// GetCompletionFunc retrieves the TaskCompletionFunc from the context, if it exists.
func GetCompletionFunc(ctx context.Context) (TaskCompletionFunc, bool) {
	f, ok := ctx.Value(taskCompletionFuncKey{}).(TaskCompletionFunc)
	return f, ok
}

// Option configures durable command persistence, retries, and sweeping.
type Option func(*config)

type config struct {
	maxRetries    int
	retryBackoff  backoff.Config
	sweepInterval time.Duration
	stuckTimeout  time.Duration
	exporter      dl.Exporter
}

func defaultConfig() config {
	return config{
		maxRetries:    5,
		retryBackoff:  backoff.DefaultConfig(),
		sweepInterval: defaultSweepInterval,
		stuckTimeout:  defaultStuckTimeout,
	}
}

// WithMaxRetries sets the maximum number of retries after the initial attempt.
func WithMaxRetries(maxRetries int) Option {
	return func(c *config) {
		c.maxRetries = maxRetries
	}
}

// WithRetryBackoff sets the retry delay curve. The pattern accepts STD, EXP,
// PROG, FIXED, or comma-separated durations.
func WithRetryBackoff(pattern string) Option {
	return func(c *config) {
		cfg := backoff.ParseConfig(pattern)
		c.retryBackoff = cfg
		if cfg.MaxRetries > 0 {
			c.maxRetries = cfg.MaxRetries
		}
	}
}

// WithSweepInterval sets how often the background sweeper checks retryable and
// stuck tasks.
func WithSweepInterval(interval time.Duration) Option {
	return func(c *config) {
		c.sweepInterval = interval
	}
}

// WithStuckTimeout sets how long a processing task can stay locked before the
// sweeper treats it as interrupted.
func WithStuckTimeout(timeout time.Duration) Option {
	return func(c *config) {
		c.stuckTimeout = timeout
	}
}

// WithDeadLetterExporter registers a best-effort exporter called after a
// command dead_letters row is inserted. Export failures do not roll back DB.
func WithDeadLetterExporter(exporter dl.Exporter) Option {
	return func(c *config) {
		c.exporter = exporter
	}
}

// Middleware implements a durable command handling middleware that persists
// commands before passing them to the next handler.
type Middleware struct {
	db         *sql.DB
	codec      eh.CommandCodec
	cfg        config
	insertStmt *sql.Stmt
}

// NewMiddleware creates a new durable middleware and ensures the required
// async_tasks and dead_letters tables exist.
func NewMiddleware(db *sql.DB, options ...Option) (eh.CommandHandlerMiddleware, error) {
	cfg := applyOptions(options...)
	if err := ensureSchema(db); err != nil {
		return nil, err
	}

	insertStmt, err := db.Prepare(`
		INSERT INTO async_tasks (task_uuid, command_type, command_blob, status, retry_count, max_retries, created_at, updated_at)
		VALUES (?, ?, ?, 'new', 0, ?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("durable: could not prepare insert statement: %w", err)
	}

	m := &Middleware{
		db:         db,
		codec:      json.CommandCodec{},
		cfg:        cfg,
		insertStmt: insertStmt,
	}

	return func(h eh.CommandHandler) eh.CommandHandler {
		return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
			return m.handler(ctx, cmd, h)
		})
	}, nil
}

func applyOptions(options ...Option) config {
	cfg := defaultConfig()
	for _, option := range options {
		option(&cfg)
	}
	return cfg
}

func ensureSchema(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS async_tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			task_uuid TEXT NOT NULL UNIQUE,
			command_type TEXT NOT NULL,
			command_blob TEXT NOT NULL,
			status TEXT NOT NULL CHECK(status IN ('new', 'processing', 'completed', 'failed_retriable', 'failed_permanent')),
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 5,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			last_error TEXT,
			locked_by TEXT,
			locked_at TIMESTAMP,
			next_retry_at TIMESTAMP
		);
	`); err != nil {
		return fmt.Errorf("durable: could not create async_tasks table: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_async_tasks_lookup ON async_tasks (status, next_retry_at);`); err != nil {
		return fmt.Errorf("durable: could not create async_tasks_lookup index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_async_tasks_uuid ON async_tasks (task_uuid);`); err != nil {
		return fmt.Errorf("durable: could not create async_tasks_uuid index: %w", err)
	}
	if err := deadletter.EnsureSchema(db, "dead_letters"); err != nil {
		return fmt.Errorf("durable: %w", err)
	}
	// One-time rewrite of async_tasks timestamps into the canonical UTC text
	// form; next_retry_at / locked_at are compared as text in Sweep. Marker
	// guarded, fail closed.
	ctx := context.Background()
	if _, err := schema.Apply(ctx, db, "durable", "utc_timestamps", func(tx *sql.Tx) error {
		for _, col := range []string{"created_at", "updated_at", "locked_at", "next_retry_at"} {
			if _, err := schema.NormalizeColumnUTC(ctx, tx, "async_tasks", col); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("durable: could not normalize async_tasks timestamps: %w", err)
	}
	return nil
}

func (m *Middleware) handler(ctx context.Context, cmd eh.Command, h eh.CommandHandler) error {
	if taskID, ok := ctx.Value(taskIDContextKey{}).(int64); ok {
		return m.handleTask(ctx, taskID, cmd, h)
	}

	taskUUID := uuid.New()
	now := schema.UTC(time.Now())
	cmdBlob, err := m.codec.MarshalCommand(ctx, cmd)
	if err != nil {
		return fmt.Errorf("durable: could not marshal command: %w", err)
	}
	res, err := m.insertStmt.ExecContext(ctx, taskUUID.String(), cmd.CommandType().String(), cmdBlob, m.cfg.maxRetries, now, now)
	if err != nil {
		return fmt.Errorf("durable: could not save command to queue: %w", err)
	}
	taskID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("durable: could not get last insert ID: %w", err)
	}

	return m.handleTask(ctx, taskID, cmd, h)
}

func (m *Middleware) handleTask(ctx context.Context, taskID int64, cmd eh.Command, h eh.CommandHandler) (err error) {
	if err := m.markProcessing(ctx, taskID); err != nil {
		return err
	}

	var completed atomic.Bool
	completionFunc := func(status string, execErr error) {
		if completed.Swap(true) {
			return
		}
		if err := m.completeTask(ctx, taskID, cmd, status, execErr); err != nil {
			log.Printf("durable middleware: CRITICAL: failed to update task status for taskID %d: %v", taskID, err)
		}
	}
	taskCtx := context.WithValue(ctx, taskCompletionFuncKey{}, TaskCompletionFunc(completionFunc))

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("durable: panic recovered in handler: %v", recovered)
			completionFunc(statusFailedPermanent, err)
		}
	}()

	err = h.HandleCommand(taskCtx, cmd)
	if !completed.Load() {
		if err != nil {
			if getSeverity(err) == SeverityFatal {
				completionFunc(statusFailedPermanent, err)
			} else {
				completionFunc(statusFailedRetriable, err)
			}
		} else {
			completionFunc(statusCompleted, nil)
		}
	}
	return err
}

func (m *Middleware) markProcessing(ctx context.Context, taskID int64) error {
	now := schema.UTC(time.Now())
	if _, err := m.db.ExecContext(ctx, `
		UPDATE async_tasks
		SET status = 'processing', updated_at = ?, locked_by = ?, locked_at = ?, next_retry_at = NULL
		WHERE id = ?
	`, now, lockOwner(), now, taskID); err != nil {
		return fmt.Errorf("durable: could not update task %d to processing: %w", taskID, err)
	}
	return nil
}

func (m *Middleware) completeTask(ctx context.Context, taskID int64, cmd eh.Command, status string, execErr error) error {
	switch status {
	case statusCompleted:
		return m.updateFinalStatus(ctx, taskID, statusCompleted, execErr)
	case statusFailedPermanent:
		return m.markPermanent(ctx, taskID, cmd, execErr)
	case statusFailedRetriable:
		return m.scheduleRetry(ctx, taskID, cmd, execErr)
	default:
		return m.updateFinalStatus(ctx, taskID, status, execErr)
	}
}

func (m *Middleware) updateFinalStatus(ctx context.Context, taskID int64, status string, execErr error) error {
	errMsg := errorString(execErr)
	if _, err := m.db.ExecContext(ctx, `
		UPDATE async_tasks
		SET status = ?, last_error = ?, updated_at = ?, locked_by = NULL, locked_at = NULL, next_retry_at = NULL
		WHERE id = ?
	`, status, errMsg, schema.UTC(time.Now()), taskID); err != nil {
		return fmt.Errorf("durable: could not update task %d status: %w", taskID, err)
	}
	return nil
}

func (m *Middleware) scheduleRetry(ctx context.Context, taskID int64, cmd eh.Command, execErr error) error {
	task, err := loadTask(ctx, m.db, taskID)
	if err != nil {
		return err
	}
	if task.retryCount >= task.maxRetries {
		return m.markPermanent(ctx, taskID, cmd, execErr)
	}

	nextRetryCount := task.retryCount + 1
	nextRetryAt := schema.UTC(time.Now().Add(m.cfg.retryBackoff.DelayFunc(int64(nextRetryCount))))
	errMsg := errorString(execErr)
	if _, err := m.db.ExecContext(ctx, `
		UPDATE async_tasks
		SET status = 'failed_retriable', retry_count = ?, last_error = ?, updated_at = ?, locked_by = NULL, locked_at = NULL, next_retry_at = ?
		WHERE id = ?
	`, nextRetryCount, errMsg, schema.UTC(time.Now()), nextRetryAt, taskID); err != nil {
		return fmt.Errorf("durable: could not schedule retry for task %d: %w", taskID, err)
	}
	return nil
}

func (m *Middleware) markPermanent(ctx context.Context, taskID int64, cmd eh.Command, execErr error) error {
	if err := m.updateFinalStatus(ctx, taskID, statusFailedPermanent, execErr); err != nil {
		return err
	}
	task, err := loadTask(ctx, m.db, taskID)
	if err != nil {
		return err
	}
	return insertCommandDeadLetter(ctx, m.db, task, cmd, execErr, m.cfg.exporter)
}

// Resume dispatches unfinished commands from the database to the command bus.
// The bus may be wrapped with this durable middleware; existing task context
// prevents duplicate async_tasks rows during re-dispatch.
func Resume(ctx context.Context, db *sql.DB, bus eh.CommandHandler, options ...Option) error {
	if err := ensureSchema(db); err != nil {
		return err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT id, command_blob
		FROM async_tasks
		WHERE status = 'new' OR status = 'processing'
		ORDER BY created_at ASC, id ASC
	`)
	if err != nil {
		return fmt.Errorf("durable: could not query for unfinished tasks: %w", err)
	}
	defer rows.Close()

	tasks, err := scanDispatchTasks(rows)
	if err != nil {
		return err
	}
	return dispatchTasks(ctx, tasks, bus)
}

// StartSweeper launches a background retry sweeper. The caller controls its
// lifetime through ctx.
func StartSweeper(ctx context.Context, db *sql.DB, bus eh.CommandHandler, options ...Option) error {
	if err := ensureSchema(db); err != nil {
		return err
	}
	cfg := applyOptions(options...)
	go func() {
		ticker := time.NewTicker(cfg.sweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if _, err := Sweep(ctx, db, bus, options...); err != nil {
					log.Printf("durable: sweeper failed: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Sweep dispatches retryable due tasks and stuck processing tasks once.
func Sweep(ctx context.Context, db *sql.DB, bus eh.CommandHandler, options ...Option) (int, error) {
	if err := ensureSchema(db); err != nil {
		return 0, err
	}
	cfg := applyOptions(options...)
	now := schema.UTC(time.Now())
	stuckBefore := schema.UTC(now.Add(-cfg.stuckTimeout))
	rows, err := db.QueryContext(ctx, `
		SELECT id, command_blob
		FROM async_tasks
		WHERE (status = 'failed_retriable' AND next_retry_at <= ?)
		   OR (status = 'processing' AND locked_at IS NOT NULL AND locked_at < ?)
		ORDER BY updated_at ASC, id ASC
	`, now, stuckBefore)
	if err != nil {
		return 0, fmt.Errorf("durable: could not query retryable tasks: %w", err)
	}
	defer rows.Close()

	tasks, err := scanDispatchTasks(rows)
	if err != nil {
		return 0, err
	}
	if err := dispatchTasks(ctx, tasks, bus); err != nil {
		return len(tasks), err
	}
	return len(tasks), nil
}

type dispatchTask struct {
	id          int64
	commandBlob []byte
}

func scanDispatchTasks(rows *sql.Rows) ([]dispatchTask, error) {
	var tasks []dispatchTask
	for rows.Next() {
		var task dispatchTask
		if err := rows.Scan(&task.id, &task.commandBlob); err != nil {
			return nil, fmt.Errorf("durable: could not scan task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("durable: error during row iteration: %w", err)
	}
	return tasks, nil
}

func dispatchTasks(ctx context.Context, tasks []dispatchTask, bus eh.CommandHandler) error {
	codec := json.CommandCodec{}
	for _, task := range tasks {
		cmd, cmdCtx, err := codec.UnmarshalCommand(ctx, task.commandBlob)
		if err != nil {
			log.Printf("durable: could not unmarshal command for task %d: %v", task.id, err)
			continue
		}
		if cmdCtx == nil {
			cmdCtx = ctx
		}
		cmdCtx = context.WithValue(cmdCtx, taskIDContextKey{}, task.id)
		if err := bus.HandleCommand(cmdCtx, cmd); err != nil {
			log.Printf("durable: could not re-dispatch command for task %d: %v", task.id, err)
		}
	}
	return nil
}

type taskRecord struct {
	id          int64
	taskUUID    string
	commandType string
	commandBlob []byte
	retryCount  int
	maxRetries  int
	createdAt   time.Time
}

func loadTask(ctx context.Context, db *sql.DB, taskID int64) (taskRecord, error) {
	var task taskRecord
	if err := db.QueryRowContext(ctx, `
		SELECT id, task_uuid, command_type, command_blob, retry_count, max_retries, created_at
		FROM async_tasks
		WHERE id = ?
	`, taskID).Scan(&task.id, &task.taskUUID, &task.commandType, &task.commandBlob, &task.retryCount, &task.maxRetries, &task.createdAt); err != nil {
		return taskRecord{}, fmt.Errorf("durable: could not load task %d: %w", taskID, err)
	}
	return task, nil
}

func insertCommandDeadLetter(ctx context.Context, db *sql.DB, task taskRecord, cmd eh.Command, execErr error, exporter dl.Exporter) error {
	reason := "command failed permanently"
	if execErr != nil {
		reason = execErr.Error()
	}
	record := dl.Record{
		ID:                uuid.New().String(),
		Source:            "command",
		EventType:         task.commandType,
		AggregateID:       cmd.AggregateID().String(),
		HandlerType:       "command_handler",
		RemainingHandlers: "[]",
		Blob:              string(task.commandBlob),
		Error:             reason,
		RetryCount:        task.retryCount,
		CreatedAt:         schema.UTC(task.createdAt),
		DeadAt:            schema.UTC(time.Now()),
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (id, source, event_type, aggregate_id, handler_type, outbox_id, remaining_handlers, blob, error, retry_count, created_at, dead_at)
		VALUES (?, 'command', ?, ?, 'command_handler', NULL, '[]', ?, ?, ?, ?, ?)
	`, record.ID, record.EventType, record.AggregateID, record.Blob, record.Error, record.RetryCount, record.CreatedAt, record.DeadAt); err != nil {
		return fmt.Errorf("durable: could not insert command dead letter: %w", err)
	}
	if exporter != nil {
		if err := exporter.ExportDeadLetter(ctx, record); err != nil {
			log.Printf("durable: could not export command dead letter: %v", err)
		} else if err := markCommandDeadLetterExported(ctx, db, record.ID, schema.UTC(time.Now())); err != nil {
			log.Printf("durable: could not mark command dead letter exported: %v", err)
		}
	}
	return nil
}

func markCommandDeadLetterExported(ctx context.Context, db *sql.DB, id string, exportedAt time.Time) error {
	if _, err := db.ExecContext(ctx, `UPDATE dead_letters SET exported_at = ? WHERE id = ?`, schema.UTC(exportedAt), id); err != nil {
		return err
	}
	return nil
}

func errorString(err error) sql.NullString {
	if err == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: err.Error(), Valid: true}
}

func getSeverity(err error) ErrorSeverity {
	var categorized CategorizedError
	if errors.As(err, &categorized) {
		return categorized.DurableSeverity()
	}
	return SeverityRetryable
}

func lockOwner() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "unknown"
	}
	return hostname
}
