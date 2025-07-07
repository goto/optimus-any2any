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

// ConnectorGetter is an interface that defines methods similar to component.Getter
// but specifically for the Connector component.
type ConnectorGetter Getter

// Connector is a struct that useful for connecting source and multiple sinks.
type Connector struct {
	*Base
	component     string
	name          string
	connectorFunc ConnectorFunc
}

var _ ConnectorGetter = (*Connector)(nil)

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

// Context returns the context of the Connector.
func (c *Connector) Context() context.Context {
	return c.ctx
}

// Component returns the component name of the Connector.
func (c *Connector) Component() string {
	return c.component
}

// Name returns the name of the Connector.
func (c *Connector) Name() string {
	return c.name
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
