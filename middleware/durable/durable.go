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
	"fmt"
	"time"

	json "github.com/json-iterator/go"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/uuid"
)

// --- Unexported context keys and types ---
type taskCompletionFuncKey struct{}

// TaskCompletionFunc is a function that signals the completion status of a task.
type TaskCompletionFunc func(status string, execErr error)

// --- Public function to retrieve the callback ---

// GetCompletionFunc retrieves the TaskCompletionFunc from the context, if it exists.
func GetCompletionFunc(ctx context.Context) (TaskCompletionFunc, bool) {
	f, ok := ctx.Value(taskCompletionFuncKey{}).(TaskCompletionFunc)
	return f, ok
}

// Middleware implements a durable command handling middleware that persists commands
// to a database before passing them to the next handler. It provides a callback
// in the context to update the task's status upon completion.
type Middleware struct {
	db         *sql.DB
	insertStmt *sql.Stmt
	updateStmt *sql.Stmt
}

// NewMiddleware creates a new Durable Middleware.
// It also ensures the required `async_tasks` table exists in the database.
func NewMiddleware(db *sql.DB) (eh.CommandHandlerMiddleware, error) {
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

	m := &Middleware{
		db:         db,
		insertStmt: insertStmt,
		updateStmt: updateStmt,
	}

	return func(h eh.CommandHandler) eh.CommandHandler {
		return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) error {
			return m.handler(ctx, cmd, h)
		})
	}, nil
}

func (m *Middleware) handler(ctx context.Context, cmd eh.Command, h eh.CommandHandler) error {
	// 1. Save the command to the database.
	taskUUID := uuid.New()
	now := time.Now()

	cmdBlob, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("durable: could not marshal command: %w", err)
	}
	res, err := m.insertStmt.ExecContext(ctx, taskUUID.String(), cmd.CommandType().String(), string(cmdBlob), now, now)
	if err != nil {
		return fmt.Errorf("durable: could not save command to queue: %w", err)
	}
	taskID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("durable: could not get last insert ID: %w", err)
	}

	// 2. Define the completion callback.
	completionFunc := func(status string, execErr error) {
		var errMsg sql.NullString
		if execErr != nil {
			errMsg.String = execErr.Error()
			errMsg.Valid = true
		}
		// Each Exec call on a DB object runs in its own implicit transaction (autocommit).
		m.updateStmt.Exec(status, errMsg, time.Now(), taskID)
	}

	// 3. Enrich the context with the callback.
	taskCtx := context.WithValue(ctx, taskCompletionFuncKey{}, TaskCompletionFunc(completionFunc))

	// 4. Call the next handler in the chain.
	return h.HandleCommand(taskCtx, cmd)
}
