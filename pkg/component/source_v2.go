package component

import (
	"io"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type CoreSourceV2 struct {
	*CoreV2
	r io.ReadCloser
	w io.WriteCloser
}

var _ flow.SourceV2 = (*CoreSourceV2)(nil)

func NewCoreSourceV2(l *slog.Logger, name string) *CoreSourceV2 {
	r, w := io.Pipe()
	c := &CoreSourceV2{
		CoreV2: NewCoreV2(l, "source", name),
		r:      r,
		w:      w,
	}
	c.CoreV2.postHookProcess = func() error {
		// close the writer when the process is done
		c.l.Debug("close")
		return c.w.Close()
	}
	return c
}

func (c *CoreSourceV2) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (c *CoreSourceV2) Close() error {
	c.r.Close()
	return c.CoreV2.Close()
}

func (c *CoreSourceV2) Send(v any) {
	_, err := c.w.Write(append(v.([]byte), '\n'))
	if err != nil {
		c.err = err
	}
}
