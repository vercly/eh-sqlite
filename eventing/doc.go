// Package eventing provides shared logic for event-driven components
// within the amlcheck domain, focusing on command and event tracing.
//
// # Usage
//
// This package simplifies tracing by providing a base command and context helpers.
//
// ## Base Command
//
// To ensure all commands have a unique ID (for causation tracking), embed
// `eventing.BaseCommand` in your command structs. The `AggregateID` is also
// provided by `BaseCommand` and is automatically set to the command's unique ID.
//
// Example:
//
//	import (
//		"internal/amlcheck/eventing"
//		eh "github.com/vercly/eventhorizon"
//	)
//
//	const MyCommand = eh.CommandType("my:command")
//
//	type MyCmd struct {
//		eventing.BaseCommand
//		// other fields...
//	}
//
//	func (c *MyCmd) AggregateType() eh.AggregateType { return "my_aggregate" }
//	func (c *MyCmd) CommandType() eh.CommandType      { return MyCommand }
//
//
// ## Tracing Context
//
// To trace a flow of operations, use the correlation ID helpers.
// The correlation ID will be automatically added to the metadata of all
// events created within a context that carries it.
//
// ### Starting a Trace
//
// Use `WithCorrelationID` to add a new correlation ID to the context,
// typically at the start of an operation (e.g., in an HTTP handler).
//
//	ctx := eventing.WithCorrelationID(context.Background(), "my-unique-correlation-id")
//
// ### In a Command Handler
//
// When creating an event, ensure you pass the context to the aggregate's
// command handler. To propagate tracing info, create event options for
// `causation_id` (from the command) and `correlation_id` (from the context).
//
//	func (a *MyAggregate) HandleCommand(ctx context.Context, command eh.Command) error {
//		// ...
//		options := []eh.EventOption{eh.FromCommand(cmd)}
//		if correlationID, ok := eventing.CorrelationIDFromContext(ctx); ok {
//			options = append(options, eh.WithMetadata(map[string]any{
//				"correlation_id": correlationID,
//			}))
//		}
//		a.AppendEvent(MyEventType, MyEventData{}, time.Now(), options...)
//		// ...
//	}
package eventing
