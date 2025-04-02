package component

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// Registrants is an interface that defines the methods
// that a component must implement to register processes and clean functions.
type Registrants interface {
	AddCleanFunc(f func() error)
	RegisterProcess(f func() error)
}

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

// Core is a struct that implements the Registrants and Setter interfaces.
type Core struct {
	*Base
	backend
	backendName     string
	size            int
	component       string
	name            string
	postHookProcess func() error // this is called after all processes are done
}

var _ Registrants = (*Core)(nil)
var _ Setter = (*Core)(nil)
var _ flow.Inlet = (*Core)(nil)
var _ flow.Outlet = (*Core)(nil)

// NewCore creates a new Core instance.
func NewCore(l *slog.Logger, component, name string) *Core {
	c := &Core{
		Base:            NewBase(l),
		backendName:     "channel",
		size:            0,
		component:       component,
		name:            name,
		postHookProcess: func() error { return nil },
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

// AddCleanFunc adds a clean function to the component.
// Clean functions are called when the component is closed
// whether it is closed gracefully or due to an error.
func (c *Core) AddCleanFunc(f func() error) {
	c.cleanFuncs = append(c.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function is expected to return an error if it fails.
// And postHookProcess is called after all processes are done.
func (c *Core) RegisterProcess(f func() error) {
	go func() {
		defer func() {
			c.postHookProcess()
		}()
		if err := f(); err != nil {
			c.l.Error(fmt.Sprintf("process error: %s", err.Error()))
			c.err = errors.WithStack(err)
		}
	}()
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
