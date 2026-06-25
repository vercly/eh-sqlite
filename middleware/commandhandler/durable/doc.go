/*
Package durable provides an eventhorizon CommandHandlerMiddleware that persists
each command to an async_tasks table before it executes, so commands survive a
crash and can be resumed.

When combined with the async middleware (durable outermost, async inner), the
command's outcome is not known by the time the durable middleware returns —
execution is deferred to a goroutine. The middleware therefore injects a
TaskCompletionFunc into the context; the executing handler must report the final
status by calling it, either directly via GetCompletionFunc or through the
tracing.NewDurableHandler wrapper, which does it automatically.

Retry: a handler that fails with a retriable error (the default; opt out by
returning a CategorizedError reporting SeverityPermanent) is moved to
failed_retriable with an incremented retry_count and a next_retry_at backoff, until
max_retries is reached, after which it becomes failed_permanent. Configure the delay
with WithRetryBackoff.

Recovery: Resume re-dispatches interrupted (new/processing) and due failed_retriable
tasks, reusing their existing rows. Call it after all command handlers are
registered, and periodically (for example from a cron job) to drive retries during
normal operation.

Example wiring:

	durableMW, err := durable.NewMiddleware(db) // db is a *sql.DB the caller owns
	if err != nil {
		log.Fatal(err)
	}
	asyncMW, _ := async.NewMiddleware()

	// durable is outermost so the command is persisted before async defers it.
	bus := eh.UseCommandHandlerMiddleware(handler, durableMW, asyncMW)
*/
package durable
