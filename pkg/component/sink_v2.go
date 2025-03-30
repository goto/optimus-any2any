package component

import (
	"bufio"
	"io"
	"iter"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type CoreSinkV2 struct {
	*CoreV2
	w    io.WriteCloser
	r    io.ReadCloser
	done chan uint8
}

var _ flow.SinkV2 = (*CoreSinkV2)(nil)

func NewCoreSinkV2(l *slog.Logger, name string) *CoreSinkV2 {
	r, w := io.Pipe()
	c := &CoreSinkV2{
		CoreV2: NewCoreV2(l, "sink", name),
		w:      w,
		r:      r,
		done:   make(chan uint8),
	}

	c.CoreV2.postHookProcess = func() error {
		for _ = range c.Read() {
			// drain the if it still contains messages
		}
		c.done <- 0
		return nil
	}
	return c
}

func (c *CoreSinkV2) Read() iter.Seq[any] {
	sc := bufio.NewScanner(c.r)
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

func (c *CoreSinkV2) Write(p []byte) (n int, err error) {
	return c.w.Write(p)
}

func (c *CoreSinkV2) Close() error {
	c.w.Close()
	return c.CoreV2.Close()
}

func (c *CoreSinkV2) Wait() {
	<-c.done
	close(c.done)
}
