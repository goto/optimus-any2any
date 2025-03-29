package component

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type CoreSource struct {
	*Core
}

var _ flow.Source = (*CoreSource)(nil)

func NewCoreSource(l *slog.Logger, name string) *CoreSource {
	c := &CoreSource{
		Core: NewCore(l, "source", name),
	}
	// special case for source to close the channel
	// after all processes are done
	c.postHookProcess = func() error {
		close(c.c)
		c.l.Debug(fmt.Sprintf("close success"))
		return nil
	}
	return c
}

func (c *CoreSource) Component() string {
	return "source"
}

func (c *CoreSource) Out() <-chan any {
	return c.c
}

func (c *CoreSource) Send(v any) {
	c.c <- v
}
