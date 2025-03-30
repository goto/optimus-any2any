package component

import (
	"fmt"
	"log/slog"

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
type Setter interface {
	SetLogger(l *slog.Logger)
}

// Core is a struct that implements the Registrants and Setter interfaces.
type Core struct {
	*Base
	component       string
	name            string
	postHookProcess func() error // this is called after all processes are done
}

var _ Registrants = (*Core)(nil)
var _ Setter = (*Core)(nil)

// NewCore creates a new Core instance.
func NewCore(l *slog.Logger, component, name string) *Core {
	c := &Core{
		Base:            NewBase(l),
		component:       component,
		name:            name,
		postHookProcess: func() error { return nil },
	}
	c.l = c.l.WithGroup(c.component).With("name", c.name)
	return c
}

// SetLogger sets the logger for the core component.
func (c *Core) SetLogger(l *slog.Logger) {
	c.l = l
	c.l = c.l.WithGroup(c.component).With("name", c.name)
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
