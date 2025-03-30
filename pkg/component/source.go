package component

import (
	"bufio"
	errs "errors"
	"fmt"
	"io"
	"iter"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// CoreSource is an implementation of the source interface.
type CoreSource struct {
	*Core
	r io.ReadCloser
	w io.WriteCloser
}

var _ flow.Source = (*CoreSource)(nil)

// NewCoreSource creates a new CoreSource instance.
func NewCoreSource(l *slog.Logger, name string) *CoreSource {
	r, w := io.Pipe()
	c := &CoreSource{
		Core: NewCore(l, "source", name),
		r:    r,
		w:    w,
	}
	// special case for source to close the channel
	// after all processes are done
	c.Core.postHookProcess = func() error {
		// close the writer when the process is done
		c.Core.l.Debug(fmt.Sprintf("close source writer"))
		return c.w.Close()
	}
	return c
}

// Out returns the iterator to read from the source
func (c *CoreSource) Out() iter.Seq[any] {
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

// Send sends a value to the writer
func (c *CoreSource) Send(v any) {
	_, err := c.w.Write(append(v.([]byte), '\n'))
	if err != nil && !errs.Is(err, io.ErrClosedPipe) {
		c.Core.Logger().Warn(fmt.Sprintf("failed to write to source: %s", err.Error()))
	}
}

// Close closes the source and underlying reader
func (c *CoreSource) Close() error {
	c.w.Close()
	return c.Core.Close()
}
