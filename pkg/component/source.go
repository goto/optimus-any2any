package component

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// CoreSource is an implementation of the source interface.
type CoreSource struct {
	*Core
}

var _ flow.Source = (*CoreSource)(nil)

// NewCoreSource creates a new CoreSource instance.
func NewCoreSource(l *slog.Logger, name string) *CoreSource {
	c := &CoreSource{
		Core: NewCore(l, "source", name),
	}
	// special case for source to close the channel
	// after all processes are done
	c.Core.postHookProcess = func() error {
		close(c.Core.c)
		c.Core.l.Debug(fmt.Sprintf("close success"))
		return nil
	}
	return c
}

// Out returns the channel to read from
func (c *CoreSource) Out() <-chan any {
	return c.Core.c
}

// Send sends a value to the channel
func (c *CoreSource) Send(v any) {
	c.Core.c <- v
}
