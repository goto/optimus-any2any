package sftp

import (
	"context"
	"io"
	"log/slog"

	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/pkg/errors"
)

type sftpHandler struct {
	*xio.CommonWriteHandler
	client *Client
}

var _ xio.WriteHandler = (*sftpHandler)(nil)

func NewSFTPHandler(ctx context.Context, logger *slog.Logger, address, username, password, privateKey, hostFingerprint string,
	enableOverwrite bool, opts ...xio.Option) xio.WriteHandler {
	w := xio.NewCommonWriteHandler(ctx, logger, opts...)
	s := &sftpHandler{
		CommonWriteHandler: w,
		client:             nil,
	}

	// set appropriate schema and writer function
	s.SetSchema("sftp")
	s.SetNewWriterFunc(func(destinationURI string) (io.Writer, error) {
		if s.client == nil {
			// create a new SFTP client
			c, err := NewSFTPClient(address, username, password, privateKey, hostFingerprint)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			s.client = c
		}
		// remove file if it exists and overwrite is enabled
		if enableOverwrite {
			if err := s.client.Remove(destinationURI); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		// open or create file for writing
		return s.client.NewWriter(destinationURI)
	})

	return s
}
