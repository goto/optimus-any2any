package component

import (
	"iter"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// Reader is an interface that defines a method to read data inside a sink.
type Reader interface {
	Read() iter.Seq[[]byte]
}

// CoreSink is an implementation of the sink interface.
type CoreSink struct {
	*Core
	done chan uint8
}

var _ flow.Sink = (*CoreSink)(nil)
var _ Reader = (*CoreSink)(nil)

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
		for range c.Out() {
			// drain the channel
		}
		c.Core.Logger().Debug("process done")
		c.done <- 0
		return nil
	}
	return c
}

// Read reads the data from the sink.
func (c *CoreSink) Read() iter.Seq[[]byte] {
	return c.Core.Out()
}

// Wait waits for the sink to finish processing
func (c *CoreSink) Wait() {
	<-c.done
	close(c.done)
}
