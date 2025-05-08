package component

import (
	"context"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// CoreSink is an implementation of the sink interface.
type CoreSink struct {
	*Core
	done chan uint8
}

var _ flow.Sink = (*CoreSink)(nil)

func NewCoreSink(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, name string) *CoreSink {
	c := &CoreSink{
		Core: NewCore(ctx, cancelFn, l, "sink", name),
		done: make(chan uint8),
	}
	// special case for sink to drain the channel after process is done
	// and send done signal
	// this is to prevent the sink from blocking
	c.Core.postHookProcess = func() error {
		c.Core.Logger().Info("skip message")
		for range c.Out() {
			// drain the channel
		}
		c.Core.Logger().Info("post hook process done")
		c.done <- 0
		return nil
	}
	return c
}

// Wait waits for the sink to finish processing
func (c *CoreSink) Wait() {
	// wait for the sink to finish processing
	c.Core.Logger().Info("waiting for sink to finish processing")
	<-c.done
	c.Core.Logger().Info("sink finished processing")
	close(c.done)
	c.Core.Logger().Info("sink done channel closed")
}
