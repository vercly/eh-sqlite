// Package eventing provides small, domain-agnostic helpers for building
// eventhorizon-based applications: a reusable base command type and correlation-ID
// propagation between context, commands and event metadata.
//
// # Base command
//
// Embed BaseCommand to give a command a stable identity (AggregateID / CommandID)
// without boilerplate:
//
//	type MyCmd struct {
//		eventing.BaseCommand
//		// fields...
//	}
//
//	func (c *MyCmd) AggregateType() eh.AggregateType { return "my_aggregate" }
//	func (c *MyCmd) CommandType() eh.CommandType     { return "my:command" }
//
// # Correlation ID
//
// Carry a correlation ID on the context and stamp it onto emitted events so a whole
// flow can be traced and projected:
//
//	ctx = eventing.NewContextWithID(ctx, correlationID)
//
//	// when creating an event inside a handler:
//	event := eh.NewEventForAggregate(t, data, time.Now(), at, id, v,
//		eventing.WithCorrelationIDMetadata(ctx))
//
//	// later, in a projector or saga:
//	id, ok := eventing.CorrelationIDFromEvent(event)
package eventing
