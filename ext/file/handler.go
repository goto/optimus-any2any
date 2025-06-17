package file

import (
	"context"
	"io"
	"log/slog"
	"net/url"

	xio "github.com/goto/optimus-any2any/internal/io"
)

type fileHandler struct {
	*xio.CommonWriteHandler
}

var _ xio.WriteHandler = (*fileHandler)(nil)

func NewFileHandler(ctx context.Context, logger *slog.Logger, opts ...xio.Option) xio.WriteHandler {
	w := xio.NewCommonWriteHandler(ctx, logger, opts...)
	w.SetSchema("file")
	w.SetNewWriterFunc(func(destinationURI string) (io.Writer, error) {
		u, _ := url.Parse(destinationURI)
		return xio.NewWriteHandler(logger, u.Path)
	})
	return &fileHandler{w}
}
