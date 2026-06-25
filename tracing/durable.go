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

package tracing

import (
	"context"
	"fmt"
	"github.com/vercly/eh-sqlite/middleware/commandhandler/durable"
	eh "github.com/vercly/eventhorizon"
)

// NewDurableHandler wraps a command handler with logic to automatically interact
// with the durable middleware. It fetches the completion function from the context
// and calls it after the inner handler is done.
func NewDurableHandler(handler eh.CommandHandler) eh.CommandHandler {
	return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) (err error) {
		// Get the completion function from the context.
		completionFunc, ok := durable.GetCompletionFunc(ctx)
		if !ok {
			// If there's no completion function, this command is not durable.
			// We can just execute it directly.
			return handler.HandleCommand(ctx, cmd)
		}

		// Ensure the completion function is always called.
		defer func() {
			if r := recover(); r != nil {
				// A panic is a bug, not a transient fault: fail permanently.
				err = fmt.Errorf("panic recovered in durable handler: %v", r)
				completionFunc(durable.StatusFailedPermanent, err)
			}
		}()

		// Execute the actual command handler.
		err = handler.HandleCommand(ctx, cmd)

		// Report the result. Errors are retried by default; a handler can opt out by
		// returning a durable.CategorizedError reporting SeverityPermanent.
		switch {
		case err == nil:
			completionFunc(durable.StatusCompleted, nil)
		case durable.ClassifyError(err) == durable.SeverityPermanent:
			completionFunc(durable.StatusFailedPermanent, err)
		default:
			completionFunc(durable.StatusFailedRetriable, err)
		}

		return err
	})
}
