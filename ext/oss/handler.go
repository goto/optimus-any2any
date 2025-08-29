package oss

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/fs"
	"github.com/pkg/errors"
)

const (
	// DefaultTransientSuffix is the default suffix used for transient files
	DefaultTransientSuffix = "_inprogress"
)

type ossHandler struct {
	*fs.CommonWriteHandler
	client *Client
}

var _ fs.WriteHandler = (*ossHandler)(nil)

func NewOSSHandler(ctx context.Context, logger *slog.Logger, client *Client, enableOverwrite bool, opts ...fs.WriteOption) (*ossHandler, error) {
	writerFunc := func(destinationURI string) (io.Writer, error) {
		// remove object if it exists and overwrite is enabled
		if enableOverwrite {
			if err := client.Remove(destinationURI); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		// create a new writer with a transient suffix
		transientDestinationURI := destinationURI + DefaultTransientSuffix
		return client.NewWriter(transientDestinationURI)
	}

	// set appropriate schema and writer function
	w, err := fs.NewCommonWriteHandler(ctx, logger, append(opts,
		fs.WithWriteSchema("oss"), fs.WithWriteNewWriterFunc(writerFunc))...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create oss handler
	return &ossHandler{
		CommonWriteHandler: w,
		client:             client,
	}, nil
}

func (h *ossHandler) Sync() error {
	// ensure all writes are flushed
	h.CommonWriteHandler.Sync()
	// move all files from transient URIs to final URIs
	for _, destinationURI := range h.DestinationURIs() {
		if err := h.ConcurrentQueue(func() error {
			// copy the file from transient URI to final URI
			if err := h.client.Copy(destinationURI+DefaultTransientSuffix, destinationURI); err != nil {
				return errors.WithStack(err)
			}
			// remove the transient file
			if err := h.client.Remove(destinationURI + DefaultTransientSuffix); err != nil {
				return errors.WithStack(err)
			}
			h.Logger().Info(fmt.Sprintf("rename object to final uri %s is success", fs.MaskedURI(destinationURI)))
			return nil
		}); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(h.ConcurrentQueueWait()) // wait for all copy operations to finish
}
