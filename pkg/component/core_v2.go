package component

import (
	"fmt"
	"log/slog"

	"github.com/pkg/errors"
)

type CoreV2 struct {
	*Base
	component       string
	name            string
	postHookProcess func() error // this is called after all processes are done
}

var _ Registrants = (*CoreV2)(nil)

// NewCoreV2 creates a new CoreV2 instance.
func NewCoreV2(l *slog.Logger, component, name string) *CoreV2 {
	c := &CoreV2{
		Base:            NewBase(l),
		component:       component,
		name:            name,
		postHookProcess: func() error { return nil },
	}
	c.l = c.l.WithGroup(c.component).With("name", c.name)
	return c
}

func (c *CoreV2) AddCleanFunc(f func() error) {
	c.cleanFuncs = append(c.cleanFuncs, f)
}

func (c *CoreV2) RegisterProcess(f func() error) {
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
