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
	"sync/atomic"
	"time"

	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/codec/json"
	"github.com/vercly/eventhorizon/uuid"
)

// Task status values stored in the async_tasks.status column.
const (
	StatusNew             = "new"
	StatusProcessing      = "processing"
	StatusCompleted       = "completed"
	StatusFailedRetriable = "failed_retriable"
	StatusFailedPermanent = "failed_permanent"
)

// defaultRetryBackoff is how long a failed-retriable task waits before it becomes
// due for another attempt. Override with WithRetryBackoff.
const defaultRetryBackoff = 1 * time.Minute

// ErrorSeverity classifies how a failed durable command should be treated.
type ErrorSeverity int

const (
	// SeverityRetriable is the default: the command is retried until max_retries.
	SeverityRetriable ErrorSeverity = iota
	// SeverityPermanent marks an error that must not be retried.
	SeverityPermanent
)

// CategorizedError lets a command handler override the default retry classification
// by returning (or wrapping) an error that reports its own severity. Mirrors the
// outbox CategorizedError convention.
type CategorizedError interface {
	error
	DurableSeverity() ErrorSeverity
}

// ClassifyError returns the severity for err, defaulting to SeverityRetriable when
// the error does not implement CategorizedError.
func ClassifyError(err error) ErrorSeverity {
	var ce CategorizedError
	if errors.As(err, &ce) {
		return ce.DurableSeverity()
	}
	return SeverityRetriable
}

// --- Unexported context keys and types ---
type taskCompletionFuncKey struct{}

// resumeTaskIDKey carries the id of an existing async_tasks row so the middleware
// reuses it during Resume instead of inserting a duplicate row.
type resumeTaskIDKey struct{}

// TaskCompletionFunc is a function that signals the completion status of a task.
type TaskCompletionFunc func(status string, execErr error)

// --- Public function to retrieve the callback ---

// GetCompletionFunc retrieves the TaskCompletionFunc from the context, if it exists.
func GetCompletionFunc(ctx context.Context) (TaskCompletionFunc, bool) {
	f, ok := ctx.Value(taskCompletionFuncKey{}).(TaskCompletionFunc)
	return f, ok
}

// withResumeTaskID returns a context that instructs the durable middleware to reuse
// an existing task row identified by id rather than persisting a new one. It is used
// when re-dispatching commands during Resume so completion updates the original row.
func withResumeTaskID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, resumeTaskIDKey{}, id)
}

// resumeTaskID returns the existing task id carried by ctx, if any.
func resumeTaskID(ctx context.Context) (int64, bool) {
	id, ok := ctx.Value(resumeTaskIDKey{}).(int64)
	return id, ok
}

// Middleware implements a durable command handling middleware that persists commands
// to a database before passing them to the next handler. It provides a callback
// in the context to update the task's status upon completion.
type Middleware struct {
	db           *sql.DB
	codec        eh.CommandCodec
	insertStmt   *sql.Stmt
	updateStmt   *sql.Stmt
	retryStmt    *sql.Stmt
	retryBackoff time.Duration
}

// Option configures the durable Middleware.
type Option func(*Middleware)

// WithRetryBackoff sets the delay before a failed-retriable task becomes due for
// another attempt. Defaults to defaultRetryBackoff.
func WithRetryBackoff(d time.Duration) Option {
	return func(m *Middleware) { m.retryBackoff = d }
}

// NewMiddleware creates a new Durable Middleware.
// It also ensures the required `async_tasks` table exists in the database.
func NewMiddleware(db *sql.DB, opts ...Option) (eh.CommandHandlerMiddleware, error) {
	// Ensure the async_tasks table exists.
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
		return nil, fmt.Errorf("durable: could not create async_tasks table: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_async_tasks_lookup ON async_tasks (status, next_retry_at);`); err != nil {
		return nil, fmt.Errorf("durable: could not create async_tasks_lookup index: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_async_tasks_uuid ON async_tasks (task_uuid);`); err != nil {
		return nil, fmt.Errorf("durable: could not create async_tasks_uuid index: %w", err)
	}

	insertStmt, err := db.Prepare(`INSERT INTO async_tasks (task_uuid, command_type, command_blob, status, created_at, updated_at) VALUES (?, ?, ?, 'new', ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("durable: could not prepare insert statement: %w", err)
	}

	updateStmt, err := db.Prepare(`UPDATE async_tasks SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`)
	if err != nil {
		return nil, fmt.Errorf("durable: could not prepare update statement: %w", err)
	}

	// retryStmt records a retriable failure: it bumps the attempt counter, stores the
	// error and the next attempt time, and atomically flips the task to
	// failed_permanent (clearing next_retry_at) once the attempts reach max_retries.
	// The RHS expressions see the pre-update retry_count, so retry_count + 1 is the
	// new effective attempt count.
	retryStmt, err := db.Prepare(`
		UPDATE async_tasks
		SET retry_count = retry_count + 1,
			last_error = ?,
			updated_at = ?,
			status = CASE WHEN retry_count + 1 >= max_retries THEN 'failed_permanent' ELSE 'failed_retriable' END,
			next_retry_at = CASE WHEN retry_count + 1 >= max_retries THEN NULL ELSE ? END
		WHERE id = ?`)
	if err != nil {
		return nil, fmt.Errorf("durable: could not prepare retry statement: %w", err)
	}

	m := &Middleware{
		db:           db,
		codec:        json.CommandCodec{},
		insertStmt:   insertStmt,
		updateStmt:   updateStmt,
		retryStmt:    retryStmt,
		retryBackoff: defaultRetryBackoff,
	}
	for _, opt := range opts {
		opt(m)
	}

	return func(h eh.CommandHandler) eh.CommandHandler {
		return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
			return m.handler(ctx, cmd, h)
		})
	}, nil
}

func (m *Middleware) handler(ctx context.Context, cmd eh.Command, h eh.CommandHandler) error {
	// 1. Persist the command, or reuse an existing row when resuming.
	var taskID int64
	if existingID, ok := resumeTaskID(ctx); ok {
		// Resuming a previously persisted task: reuse its row so completion updates
		// the original record instead of inserting a duplicate.
		taskID = existingID
	} else {
		taskUUID := uuid.New()
		now := time.Now()

		cmdBlob, err := m.codec.MarshalCommand(ctx, cmd)
		if err != nil {
			return fmt.Errorf("durable: could not marshal command: %w", err)
		}
		res, err := m.insertStmt.ExecContext(ctx, taskUUID.String(), cmd.CommandType().String(), cmdBlob, now, now)
		if err != nil {
			return fmt.Errorf("durable: could not save command to queue: %w", err)
		}
		taskID, err = res.LastInsertId()
		if err != nil {
			return fmt.Errorf("durable: could not get last insert ID: %w", err)
		}
	}

	// 2. Define the robust completion callback.
	var completed atomic.Bool
	completionFunc := func(status string, execErr error) {
		if completed.Swap(true) {
			return // Already called.
		}

		var errMsg sql.NullString
		if execErr != nil {
			errMsg.String = execErr.Error()
			errMsg.Valid = true
		}

		now := time.Now()
		if status == StatusFailedRetriable {
			// Schedule another attempt; retryStmt flips to failed_permanent in SQL
			// once max_retries is reached.
			nextRetryAt := now.Add(m.retryBackoff)
			if _, err := m.retryStmt.Exec(errMsg, now, nextRetryAt, taskID); err != nil {
				log.Printf("durable middleware: CRITICAL: failed to record retriable failure for taskID %d: %v", taskID, err)
			}
			return
		}

		if _, err := m.updateStmt.Exec(status, errMsg, now, taskID); err != nil {
			log.Printf("durable middleware: CRITICAL: failed to update task status for taskID %d: %v", taskID, err)
		}
	}

	// 3. Enrich the context with the callback.
	taskCtx := context.WithValue(ctx, taskCompletionFuncKey{}, TaskCompletionFunc(completionFunc))

	// 4. Call the next handler in the chain.
	return h.HandleCommand(taskCtx, cmd)
}

// Resume dispatches all unfinished commands from the database to the command bus.
// This function should be called on application startup to recover from a crash,
// but only after all command handlers have been registered on the bus.
//
// The provided bus must include this durable middleware: resumed commands are
// re-dispatched carrying their existing task id, so the middleware reuses the
// original async_tasks row instead of inserting a duplicate.
func Resume(ctx context.Context, db *sql.DB, bus eh.CommandHandler) error {
	codec := json.CommandCodec{}
	// Select crash-interrupted tasks (new/processing) plus retriable tasks whose
	// backoff has elapsed and that still have attempts left.
	rows, err := db.QueryContext(ctx, `
		SELECT id, command_blob FROM async_tasks
		WHERE status = 'new'
		   OR status = 'processing'
		   OR (status = 'failed_retriable' AND next_retry_at <= ? AND retry_count < max_retries)`,
		time.Now())
	if err != nil {
		return fmt.Errorf("durable: could not query for unfinished tasks: %w", err)
	}
	defer rows.Close()

	type unfinishedTask struct {
		id          int64
		commandBlob []byte
	}

	var tasks []unfinishedTask

	// 1. Read all tasks into memory first to avoid holding the connection.
	for rows.Next() {
		var task unfinishedTask
		if err := rows.Scan(&task.id, &task.commandBlob); err != nil {
			log.Printf("durable: could not scan unfinished task: %v", err)
			continue // Try next row
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("durable: error during row iteration: %w", err)
	}
	// Close rows immediately to release the read connection.
	rows.Close()

	if len(tasks) == 0 {
		return nil // Nothing to do.
	}

	// 2. Now, process the tasks from the in-memory slice.
	updateStatusStmt, err := db.PrepareContext(ctx, `UPDATE async_tasks SET status = 'processing', updated_at = ? WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("durable: could not prepare status update statement: %w", err)
	}
	defer updateStatusStmt.Close()

	for _, task := range tasks {
		// The command codec can return a new context with data from the command (e.g. tracing).
		cmd, cmdCtx, err := codec.UnmarshalCommand(ctx, task.commandBlob)
		if err != nil {
			log.Printf("durable: could not unmarshal command for task %d: %v", task.id, err)
			continue
		}
		if cmdCtx == nil {
			cmdCtx = ctx
		}

		// Mark as processing before dispatching to avoid race conditions on quick restarts.
		if _, err := updateStatusStmt.ExecContext(ctx, time.Now(), task.id); err != nil {
			log.Printf("durable: could not update task %d to processing: %v", task.id, err)
			continue
		}

		// Re-dispatch the command carrying its existing task id so the durable
		// middleware reuses the original row instead of inserting a duplicate.
		// Use the unmarshaled context to preserve tracing/correlation data.
		if err := bus.HandleCommand(withResumeTaskID(cmdCtx, task.id), cmd); err != nil {
			log.Printf("durable: could not re-dispatch command for task %d: %v", task.id, err)
			// The task remains in 'processing' state for a sweeper to find later.
			continue
		}
	}

	return nil
}
