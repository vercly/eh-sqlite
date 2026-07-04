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

type PanicError struct {
	Value any
}

func (e PanicError) Error() string {
	return fmt.Sprintf("panic recovered in durable handler: %v", e.Value)
}

func (e PanicError) DurableSeverity() durable.ErrorSeverity {
	return durable.SeverityFatal
}

// NewDurableHandler wraps a command handler with panic recovery. Durable
// completion is handled by the durable middleware itself.
func NewDurableHandler(handler eh.CommandHandler) eh.CommandHandler {
	return eh.CommandHandlerFunc(func(ctx context.Context, cmd eh.Command) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = PanicError{Value: r}
			}
		}()

		return handler.HandleCommand(ctx, cmd)
	})
}
