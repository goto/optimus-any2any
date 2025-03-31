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
)

type backendIO struct {
	l *slog.Logger
	w io.WriteCloser
	r io.ReadCloser
}

var _ flow.Inlet = (*backendIO)(nil)
var _ flow.Outlet = (*backendIO)(nil)

func newBackendIO(l *slog.Logger, size int) *backendIO {
	bufSize := 16 * 1024
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
	sc := bufio.NewScanner(b.r)
	// return a function that takes a yield function
	return func(yield func([]byte) bool) {
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

func (b *backendIO) In(v []byte) {
	_, err := b.w.Write(append(v, '\n'))
	if err != nil && !errs.Is(err, io.ErrClosedPipe) {
		b.l.Warn(fmt.Sprintf("failed to write to sink: %s", err.Error()))
	}
}

func (b *backendIO) CloseInlet() error {
	return b.w.Close()
}
