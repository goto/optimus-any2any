package file

import (
	"fmt"
	"io"
	"log/slog"
	"net/url"

	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type FileSinkV2 struct {
	*component.CoreSinkV2
	fileHandler io.WriteCloser
}

var _ flow.SinkV2 = (*FileSinkV2)(nil)

func NewSinkV2(l *slog.Logger, destinationURI string) (*FileSinkV2, error) {
	targetURI, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if targetURI.Scheme != "file" {
		return nil, fmt.Errorf("invalid scheme: %s", targetURI.Scheme)
	}
	fh, err := NewStdFileHandler(l, targetURI.Path)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &FileSinkV2{
		CoreSinkV2:  component.NewCoreSinkV2(l, "file"),
		fileHandler: fh,
	}
	c.AddCleanFunc(func() error {
		c.Logger().Info("cleaning up file sink")
		return nil
	})
	c.RegisterProcess(c.process)
	return c, nil
}

func (c *FileSinkV2) process() error {
	for v := range c.Read() {
		raw, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("invalid type: %T", v)
		}
		if _, err := c.fileHandler.Write(append(raw, '\n')); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
