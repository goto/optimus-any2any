package component

import (
	"iter"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type backendChannel struct {
	l *slog.Logger
	c chan []byte
}

var _ flow.Inlet = (*backendChannel)(nil)
var _ flow.Outlet = (*backendChannel)(nil)

func newBackendChannel(l *slog.Logger, size int) *backendChannel {
	b := &backendChannel{
		l: l,
		c: make(chan []byte),
	}
	if size > 0 {
		b.c = make(chan []byte, size)
	}
	return b
}

func (b *backendChannel) Out() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for v := range b.c {
			if !yield(v) {
				break
			}
		}
	}
}

func (b *backendChannel) In(v []byte) {
	if v == nil {
		// skip
		return
	}
	b.c <- v
}

func (b *backendChannel) CloseInlet() error {
	close(b.c)
	return nil
}
