package component

import (
	"fmt"
	"iter"
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
	SetBufferSize(size int)
}

// Core is a struct that implements the Registrants and Setter interfaces.
type Core struct {
	*Base
	c               chan []byte
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
		c:               make(chan []byte),
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

// SetBufferSize sets the buffer size for the channel.
func (c *Core) SetBufferSize(size int) {
	if size > 0 {
		c.c = make(chan []byte, size)
	}
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

// In receives a value to the channel.
func (c *Core) In(v []byte) {
	c.c <- v
}

// CloseInlet closes the inlet.
func (c *Core) CloseInlet() error {
	close(c.c)
	return nil
}

// Out returns iterator for the channel.
func (c *Core) Out() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for v := range c.c {
			if !yield(v) {
				break
			}
		}
	}
}
