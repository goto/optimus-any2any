package sftp

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/fs"
	"github.com/pkg/errors"
)

type sftpHandler struct {
	*fs.CommonWriteHandler
	client *Client
}

var _ fs.WriteHandler = (*sftpHandler)(nil)

func NewSFTPHandler(ctx context.Context, logger *slog.Logger,
	address, username, password, privateKey, hostFingerprint string,
	enableOverwrite bool, opts ...fs.WriteOption) (*sftpHandler, error) {

	// preapare the SFTP handler
	s := &sftpHandler{client: nil}

	writeFunc := func(destinationURI string) (io.Writer, error) {
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
			if _, err := s.client.Stat(destinationURI); err == nil {
				// file exists, remove it
				if err := s.client.Remove(destinationURI); err != nil {
					return nil, errors.WithStack(err)
				}
			} else if !os.IsNotExist(err) {
				// if the error is not "file does not exist", return the error
				return nil, errors.WithStack(err)
			}
		}
		// open or create file for writing
		return s.client.NewWriter(destinationURI)
	}
	// set appropriate schema and writer function
	w, err := fs.NewCommonWriteHandler(ctx, logger, append(opts,
		fs.WithWriteSchema("sftp"), fs.WithWriteNewWriterFunc(writeFunc))...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s.CommonWriteHandler = w
	return s, nil
}
