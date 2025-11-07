package eventing

import (
	eh "github.com/vercly/eventhorizon"
	ehuuid "github.com/vercly/eventhorizon/uuid"
)

// BaseCommand is a command base for all commands in the domain.
type BaseCommand struct {
	CmdID ehuuid.UUID
}

// AggregateID returns the aggregate id.
func (c *BaseCommand) AggregateID() ehuuid.UUID {
	return c.CmdID
}

// AggregateType returns the aggregate type.
func (c *BaseCommand) AggregateType() eh.AggregateType {
	return ""
}

// CommandType returns the command type.
func (c *BaseCommand) CommandType() eh.CommandType {
	return ""
}

// CommandID returns the command id.
func (c *BaseCommand) CommandID() ehuuid.UUID {
	return c.CmdID
}

// NewBaseCommand returns a new BaseCommand.
func NewBaseCommand() BaseCommand {
	return BaseCommand{
		CmdID: ehuuid.New(),
	}
}
