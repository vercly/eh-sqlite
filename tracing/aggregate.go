// Copyright (c) 2025 - The Vercly authors.
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

// Package tracing provides base components that automatically handle event tracing.
package tracing

import (
	"context"
	"time"

	"github.com/vercly/eh-sqlite/eventing"
	eh "github.com/vercly/eventhorizon"
	"github.com/vercly/eventhorizon/aggregatestore/events"
	ehuuid "github.com/vercly/eventhorizon/uuid"
)

// ChangeApplier is an interface for the part of an aggregate that is responsible
// for applying state changes based on an event.
type ChangeApplier interface {
	ApplyChange(ctx context.Context, event eh.Event) error
}

// Tracable is an interface for aggregates that can store a correlation ID.
type Tracable interface {
	SetCorrelationID(id string)
}

// AggregateBase is a base implementation of an aggregate that automatically
// handles tracing by propagating correlation IDs from the context to events and state.
type AggregateBase struct {
	*events.AggregateBase
	applier ChangeApplier
}

// NewAggregateBase creates a new AggregateBase. It requires the concrete
// aggregate (as a ChangeApplier) to be passed in to allow the base to call
// back into its business logic.
func NewAggregateBase(aggregateType eh.AggregateType, id ehuuid.UUID, applier ChangeApplier) *AggregateBase {
	return &AggregateBase{
		AggregateBase: events.NewAggregateBase(aggregateType, id),
		applier:       applier,
	}
}

// ApplyEvent is the method called by the Event Horizon framework. It wraps the
// concrete business logic (`ApplyChange`) with automated tracing functionality.
func (a *AggregateBase) ApplyEvent(ctx context.Context, event eh.Event) error {
	// Automatically apply tracing info to the aggregate's state if it's Tracable.
	if tracable, ok := a.applier.(Tracable); ok {
		if cid, ok := eventing.CorrelationIDFromEvent(event); ok {
			tracable.SetCorrelationID(cid)
		}
	}

	// Delegate to the concrete aggregate's business logic.
	return a.applier.ApplyChange(ctx, event)
}

// AppendEventWithContext appends an event to the aggregate's list of uncommitted
// events. It automatically enriches the event with tracing information (e.g.,
// correlation ID) from the provided context.
func (a *AggregateBase) AppendEventWithContext(
	ctx context.Context,
	eventType eh.EventType,
	eventData eh.EventData,
	timestamp time.Time,
	options ...eh.EventOption,
) {
	// Automatically add tracing info from the context.
	tracingOptions := []eh.EventOption{
		eventing.WithCorrelationIDMetadata(ctx),
	}

	allOptions := append(options, tracingOptions...)

	a.AggregateBase.AppendEvent(eventType, eventData, timestamp, allOptions...)
}
