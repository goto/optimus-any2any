package component

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// ConnectorFunc is a function type that defines the signature for connecting
// a source (Outlet) to one or more sinks (Inlets).
type ConnectorFunc func(o flow.Outlet, i ...flow.Inlet) error

// Connector is a struct that useful for connecting source and multiple sinks.
type Connector struct {
	*Base
	component     string
	name          string
	connectorFunc ConnectorFunc
}

// NewConnector creates a new Connector instance.
func NewConnector(ctx context.Context, cancelFn context.CancelCauseFunc, logger *slog.Logger, name string) *Connector {
	component := "connector"
	l := logger.WithGroup(component).With("name", name)
	c := &Connector{
		Base:      NewBase(ctx, cancelFn, l),
		component: component,
		name:      name,
	}
	return c
}

// SetConnectorFunc sets the connector function for the Connector.
func (c *Connector) SetConnectorFunc(connectorFunc ConnectorFunc) {
	c.connectorFunc = connectorFunc
}

// Connect is a method that connects the source and multiple sinks.
func (c *Connector) Connect() flow.ConnectMultiSink {
	return func(o flow.Outlet, i ...flow.Inlet) {
		c.RegisterProcess(func() error {
			if c.connectorFunc == nil {
				return fmt.Errorf("connector function is not set for %s", c.name)
			}
			return c.connectorFunc(o, i...)
		})
		c.postHookProcess = func() error {
			c.l.Debug("close inlets")
			for _, inlet := range i {
				inlet.CloseInlet()
			}
			return nil
		}
		c.AddCleanFunc(func() error {
			c.l.Debug("connector close")
			return nil
		})
	}
}
