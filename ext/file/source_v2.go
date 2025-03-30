package file

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"

	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type FileSourceV2 struct {
	*component.CoreSourceV2
	r io.Reader
}

var _ flow.SourceV2 = (*FileSourceV2)(nil)

func NewSourceV2(l *slog.Logger, uri string) (*FileSourceV2, error) {
	sourceURI, err := url.Parse(uri)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceURI.Scheme != "file" {
		return nil, fmt.Errorf("invalid scheme: %s", sourceURI.Scheme)
	}
	f, err := os.OpenFile(sourceURI.Path, os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &FileSourceV2{
		CoreSourceV2: component.NewCoreSourceV2(l, "file"),
		r:            f,
	}
	c.RegisterProcess(c.process)
	return c, nil
}

func (c *FileSourceV2) process() error {
	sc := bufio.NewScanner(c.r)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)

		c.Send(line)
	}
	return c.Err()
}
