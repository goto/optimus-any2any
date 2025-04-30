package component

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// Setter is an interface that defines the methods
// that a component must implement to set some internal
// properties like logger and buffer size.
type Setter interface {
	SetLogger(l *slog.Logger)
	SetBackend(backend string)
	SetBufferSize(size int)
}

// Getter is an interface that defines the methods
// that a component must implement to get some internal
// properties like logger, component type and name.
type Getter interface {
	Context() context.Context
	Logger() *slog.Logger
	Component() string
	Name() string
}

// backend is an interface that defines Inlet and Outlet methods.
// It is used as an internal interface to abstract the backend implementation.
type backend interface {
	flow.Inlet
	flow.Outlet
}

// Core is a struct that implements the Registrants, Setter, Getter interfaces.
type Core struct {
	*Base
	backend
	backendName string
	size        int
	component   string
	name        string
}

var _ Registrants = (*Core)(nil)
var _ Setter = (*Core)(nil)
var _ Getter = (*Core)(nil)
var _ flow.Inlet = (*Core)(nil)
var _ flow.Outlet = (*Core)(nil)

// NewCore creates a new Core instance.
func NewCore(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, component, name string) *Core {
	c := &Core{
		Base:        NewBase(ctx, cancelFn, l),
		backendName: "channel",
		size:        0,
		component:   component,
		name:        name,
	}
	c.backend = c.newBackend()
	c.l = c.l.WithGroup(c.component).With("name", c.name)
	return c
}

// SetLogger sets the logger for the core component.
func (c *Core) SetLogger(l *slog.Logger) {
	c.l = l
	c.l = c.l.WithGroup(c.component).With("name", c.name)
}

// SetBufferSize sets the buffer size for the channel.
func (c *Core) SetBufferSize(size int) {
	c.l.Info(fmt.Sprintf("set buffer size to %d", size))
	c.size = size
	c.backend = c.newBackend()
}

// SetBackend sets the backend for the core component.
func (c *Core) SetBackend(backendName string) {
	c.l.Info(fmt.Sprintf("set backend to %s", backendName))
	c.backendName = backendName
	c.backend = c.newBackend()
}

// Component returns the component type of the core.
func (c *Core) Component() string {
	return c.component
}

// Name returns the name of the core component.
func (c *Core) Name() string {
	return c.name
}

// Context returns the context of the core component.
func (c *Core) Context() context.Context {
	return c.ctx
}

func (c *Core) newBackend() backend {
	switch c.backendName {
	case "channel":
		return newBackendChannel(c.l, c.size)
	case "io":
		return newBackendIO(c.l, c.size)
	default:
		c.l.Warn(fmt.Sprintf("unknown backend: %s; using channel", c.backendName))
		return newBackendChannel(c.l, c.size)
	}
}
