package component

import (
	"bufio"
	errs "errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goto/optimus-any2any/pkg/flow"
)

type backendIO struct {
	l        *slog.Logger
	w        io.WriteCloser
	r        io.ReadCloser
	isClosed atomic.Bool
	mu       sync.Mutex
}

var _ flow.Inlet = (*backendIO)(nil)
var _ flow.Outlet = (*backendIO)(nil)

func newBackendIO(l *slog.Logger, size int) *backendIO {
	bufSize := 32 * 1024
	if size > 0 {
		bufSize = size * 1024
	}

	buf := buffer.New(int64(bufSize))
	r, w := nio.Pipe(buf)

	b := &backendIO{
		l: l,
		r: r,
		w: w,
	}

	return b
}

func (b *backendIO) Out() iter.Seq[[]byte] {
	// return a function that takes a yield function
	return func(yield func([]byte) bool) {
		reader := bufio.NewReader(b.r)
		for {
			raw, err := reader.ReadBytes('\n')
			if len(raw) > 0 && raw[0] != '\n' {
				line := make([]byte, len(raw))
				copy(line, raw)

				if !yield(line) {
					return
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				b.l.Error(fmt.Sprintf("failed to read from sink: %s", err.Error()))
				break
			}
		}
	}
}

func (b *backendIO) In(v []byte) {
	if v == nil {
		// skip
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isClosed.Load() {
		return
	}
	_, err := b.w.Write(append(v, '\n'))
	if err != nil && !errs.Is(err, io.ErrClosedPipe) {
		b.l.Warn(fmt.Sprintf("failed to write to sink: %s", err.Error()))
	}
}

func (b *backendIO) CloseInlet() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.isClosed.Store(true)
	return b.w.Close()
}
