package component

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// Sender is an interface that defines a method to send data to a source.
type Sender interface {
	Send(v []byte)
}

// CoreSource is an implementation of the source interface.
type CoreSource struct {
	*Core
}

var _ flow.Source = (*CoreSource)(nil)
var _ Sender = (*CoreSource)(nil)

// NewCoreSource creates a new CoreSource instance.
func NewCoreSource(l *slog.Logger, name string) *CoreSource {
	c := &CoreSource{
		Core: NewCore(l, "source", name),
	}
	// special case for source to close the channel
	// after all processes are done
	c.Core.postHookProcess = func() error {
		c.Core.l.Debug(fmt.Sprintf("close inlet"))
		return c.Core.CloseInlet()
	}
	return c
}

// Send sends the given data to the source.
func (c *CoreSource) Send(v []byte) {
	c.Core.In(v)
}
