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

// Package correlation provides an EventHandler middleware that extracts the
// correlation ID from an event's metadata and injects it into the context
// for downstream handlers (like sagas).
package correlation

import (
	"context"
	"github.com/vercly/eh-sqlite/eventing"

	eh "github.com/vercly/eventhorizon"
)

// NewMiddleware creates a new middleware that propagates the correlation ID.
func NewMiddleware() eh.EventHandlerMiddleware {
	return func(h eh.EventHandler) eh.EventHandler {
		return &correlationMiddleware{
			next: h,
		}
	}
}

type correlationMiddleware struct {
	next eh.EventHandler
}

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
// It checks for a correlation ID in the event's metadata and, if found,
// adds it to the context before calling the next handler.
func (m *correlationMiddleware) HandleEvent(ctx context.Context, event eh.Event) error {
	nextCtx := ctx
	if correlationID, ok := eventing.CorrelationIDFromEvent(event); ok {
		nextCtx = eventing.NewContextWithID(ctx, correlationID)
	}

	return m.next.HandleEvent(nextCtx, event)
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (m *correlationMiddleware) HandlerType() eh.EventHandlerType {
	return m.next.HandlerType()
}

// InnerHandler returns the next handler in the chain.
func (m *correlationMiddleware) InnerHandler() eh.EventHandler {
	if h, ok := m.next.(eh.EventHandlerChain); ok {
		return h.InnerHandler()
	}
	return m.next
}
