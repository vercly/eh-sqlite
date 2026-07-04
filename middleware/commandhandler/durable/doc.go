/*
Package durable provides a command handler middleware that persists commands in
SQLite before execution, automatically completes tasks from handler return
values, and can retry interrupted or retryable failures with backoff.

By default, a successful handler return marks the task completed. A handler
error is retryable unless it implements CategorizedError with SeverityFatal.
Handlers that start their own async work can still retrieve TaskCompletionFunc
from the context and call it manually; the first completion wins.

Example of usage:

	package main

	import (
		"context"
		"database/sql"
		"fmt"
		"log"
		"time"

		durable "github.com/vercly/eh-sqlite/middleware/commandhandler/durable"
		eh "github.com/vercly/eventhorizon"
		"github.com/vercly/eventhorizon/commandbus/local"
		"github.com/vercly/eventhorizon/uuid"
		_ "github.com/mattn/go-sqlite3"
	)

	const MyCommandType eh.CommandType = "MyCommand"

	type MyCommand struct {
		ID uuid.UUID
	}

	func (c MyCommand) AggregateID() uuid.UUID          { return c.ID }
	func (c MyCommand) AggregateType() eh.AggregateType { return "example" }
	func (c MyCommand) CommandType() eh.CommandType     { return MyCommandType }

	type MyHandler struct{}

	func (h *MyHandler) HandleCommand(ctx context.Context, cmd eh.Command) error {
		if cmd.AggregateID() == uuid.Nil {
			return fmt.Errorf("missing aggregate id")
		}
		return nil
	}

	func main() {
		db, err := sql.Open("sqlite3", "events.db")
		if err != nil {
			log.Fatalf("could not open db: %v", err)
		}

		bus := local.New()
		durableMiddleware, err := durable.NewMiddleware(db, durable.WithRetryBackoff("EXP:1.5:5:1m"))
		if err != nil {
			log.Fatalf("could not create durable middleware: %v", err)
		}

		wrappedHandler := eh.UseCommandHandlerMiddleware(&MyHandler{}, durableMiddleware)
		bus.SetHandler(wrappedHandler, MyCommandType)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := durable.StartSweeper(ctx, db, bus); err != nil {
			log.Fatalf("could not start durable sweeper: %v", err)
		}

		cmd := MyCommand{ID: uuid.New()}
		if err := bus.HandleCommand(context.Background(), cmd); err != nil {
			log.Printf("command scheduled for retry: %v", err)
		}

		time.Sleep(500 * time.Millisecond)
	}
*/
package durable
