package component

import (
	"bufio"
	errs "errors"
	"fmt"
	"io"
	"iter"
	"log/slog"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/klauspost/readahead"
)

// CoreSink is an implementation of the sink interface.
type CoreSink struct {
	*Core
	w    io.WriteCloser
	r    io.ReadCloser
	done chan uint8
}

var _ flow.Sink = (*CoreSink)(nil)

func NewCoreSink(l *slog.Logger, name string) *CoreSink {
	buf := buffer.New(readahead.DefaultBufferSize) // 32KB In memory Buffer
	rp, w := nio.Pipe(buf)
	r, err := readahead.NewReaderSize(rp, 64, readahead.DefaultBufferSize)
	if err != nil {
		panic(fmt.Sprintf("failed to create readahead reader: %s", err.Error()))
	}

	c := &CoreSink{
		Core: NewCore(l, "sink", name),
		w:    w,
		r:    r,
		done: make(chan uint8),
	}
	// special case for sink to drain the channel after process is done
	// and send done signal
	// this is to prevent the sink from blocking
	c.Core.postHookProcess = func() error {
		c.Core.Logger().Debug("skip message")
		for _ = range c.Read() {
			// drain the if it still contains messages
		}
		c.Core.Logger().Debug("process done")
		c.done <- 0
		return nil
	}
	return c
}

// Read returns iterator to read from the sink
func (c *CoreSink) Read() iter.Seq[any] {
	sc := bufio.NewScanner(c.r)
	// return a function that takes a yield function
	return func(yield func(any) bool) {
		for sc.Scan() {
			raw := sc.Bytes()
			line := make([]byte, len(raw))
			copy(line, raw)
			if !yield(line) {
				break
			}
		}
	}
}

// In receives messages and writes them to the sink
func (c *CoreSink) In(v any) {
	_, err := c.w.Write(append(v.([]byte), '\n'))
	if err != nil && !errs.Is(err, io.ErrClosedPipe) {
		c.Core.Logger().Warn(fmt.Sprintf("failed to write to sink: %s", err.Error()))
	}
}

// CloseInlet closes the underlying writer
func (c *CoreSink) CloseInlet() error {
	return c.w.Close()
}

// Close closes the sink
func (c *CoreSink) Close() error {
	c.w.Close()
	return c.Core.Close()
}

// Wait waits for the sink to finish processing
func (c *CoreSink) Wait() {
	<-c.done
	close(c.done)
}
