package component

import (
	"log/slog"
)

// CoreSink is a helper struct that supports the sink interface.
// To comply with the sink interface, it must be declared in tandem with Core.
// Eg.
// ```go
//
//	type MySink struct {
//		*component.CoreSink
//		*component.Core
//	}
//
// ```
// MySink will be guaranteed to implement the sink interface
type CoreSink struct {
	Core *Core
	done chan uint8
}

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

// Component returns the component type of the sink
func (c *CoreSink) Component() string {
	return "sink"
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
