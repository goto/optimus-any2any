package component

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

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
	c.postHookProcess = func() error {
		c.Logger().Debug("skip message")
		for _, ok := <-c.Read(); ok; _, ok = <-c.Read() {
			// drain the channel if it still contains messages
		}
		c.Logger().Debug("process done")
		c.done <- 0
		return nil
	}
	return c
}

func (c *CoreSink) Component() string {
	return "sink"
}

func (c *CoreSink) Read() <-chan any {
	return c.c
}

func (c *CoreSink) In() chan<- any {
	return c.c
}

func (c *CoreSink) Wait() {
	<-c.done
	close(c.done)
}
