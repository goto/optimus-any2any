package component

import (
	"fmt"
	"log/slog"

	"github.com/pkg/errors"
)

type Registrants interface {
	AddCleanFunc(f func() error)
	RegisterProcess(f func() error)
}

type Core struct {
	*Base
	component       string
	name            string
	postHookProcess func() error // this is called after all processes are done
}

var _ Registrants = (*Core)(nil)

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

func (c *Core) SetLogger(l *slog.Logger) {
	c.l = l
	c.l = c.l.WithGroup(c.component).With("name", c.name)
}

func (c *Core) SetBufferSize(size int) {
	if size > 0 {
		c.c = make(chan any, size)
	}
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (c *Core) AddCleanFunc(f func() error) {
	c.cleanFuncs = append(c.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the source and send it to the channel.
// Please note that you should use the Send method to send data to the channel.
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
