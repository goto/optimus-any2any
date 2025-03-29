package component

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// CoreSink is an implementation of the sink interface.
type CoreSink struct {
	*Core
	done chan uint8
}

var _ flow.Sink = (*CoreSink)(nil)

func NewCoreSink(l *slog.Logger, name string) *CoreSink {
	c := &CoreSink{
		Core: NewCore(l, "sink", name),
		done: make(chan uint8),
	}
	// special case for sink to drain the channel after process is done
	// and send done signal
	// this is to prevent the sink from blocking
	c.Core.postHookProcess = func() error {
		c.Core.Logger().Debug("skip message")
		for _, ok := <-c.Read(); ok; _, ok = <-c.Read() {
			// drain the channel if it still contains messages
		}
		c.Core.Logger().Debug("process done")
		c.done <- 0
		return nil
	}
	return c
}

// Read returns the channel to read from
func (c *CoreSink) Read() <-chan any {
	return c.Core.c
}

// In returns the channel to write to
func (c *CoreSink) In() chan<- any {
	return c.Core.c
}

// Wait waits for the sink to finish processing
func (c *CoreSink) Wait() {
	<-c.done
	close(c.done)
}
