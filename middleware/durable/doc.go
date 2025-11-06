/*
Package durable provides a Command Handler Middleware for Event Horizon that
ensures commands are persisted before being executed. This allows for retries
and recovery in case of application failure.

The middleware saves the command to a database table (`async_tasks`) and injects
a `TaskCompletionFunc` into the context. Downstream handlers can retrieve this
function using `GetCompletionFunc` and call it to signal the final status of
the task, which updates the corresponding record in the database.

Example of usage:

	package main

	import (
		"context"
		"database/sql"
		"fmt"
		"log"
		"time"

		"github.com/vercly/eh-sqlite/middleware/durable"
		eh "github.com/vercly/eventhorizon"
		"github.com/vercly/eventhorizon/commandbus/local"
		"github.com/vercly/eventhorizon/middleware/commandhandler/async"
		_ "github.com/mattn/go-sqlite3"
	)

	// 1. Define your command.
	const MyCommandType eh.CommandType = "MyCommand"

	type MyCommand struct {
		Data string
	}

	// 2. Define your handler.
	type MyHandler struct{}

	func (h *MyHandler) HandleCommand(ctx context.Context, cmd eh.Command) error {
		// Get the completion callback from the context.
		completionFunc, ok := durable.GetCompletionFunc(ctx)
		if !ok {
			// This should not happen if the middleware is configured correctly.
			return fmt.Errorf("durable completion func not found in context")
		}

		log.Printf("handler: handling command: %s", cmd.CommandType())
		// Simulate work.
		time.Sleep(100 * time.Millisecond)

		// Signal completion.
		// The first argument is the status, the second is any execution error.
		completionFunc("completed", nil)

		return nil
	}

	func main() {
		// 3. Set up the database (in-memory SQLite for example).
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			log.Fatalf("could not open db: %v", err)
		}

		// 4. Set up the command bus.
		bus := local.New()

		// 5. Create and register the handler.
		handler := &MyHandler{}

		// 6. Create middlewares.
		asyncMiddleware, _ := async.NewMiddleware()
		durableMiddleware, err := durable.NewMiddleware(db)
		if err != nil {
			log.Fatalf("could not create durable middleware: %v", err)
		}

		// 7. Wrap the handler with all middlewares and set it on the bus.
		// The middlewares are applied from first to last, so durableMiddleware is the outermost layer.
		wrappedHandler := eh.UseCommandHandlerMiddleware(handler, durableMiddleware, asyncMiddleware)
		bus.SetHandler(wrappedHandler, MyCommandType)

		// 8. Handle a command.
		cmd := eh.NewCommand(MyCommandType, &MyCommand{Data: "hello"}, time.Now())
		if err := bus.HandleCommand(context.Background(), cmd); err != nil {
			log.Fatalf("failed to handle command: %v", err)
		}

		// The command is handled asynchronously. Give it time to complete.
		time.Sleep(500 * time.Millisecond)

		log.Println("Main: command dispatched. Check DB for 'completed' status.")
	}

*/
package durable
