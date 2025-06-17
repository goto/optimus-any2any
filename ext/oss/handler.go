package oss

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/pkg/errors"
)

type ossHandler struct {
	*xio.CommonWriteHandler
	client          *Client
	transientSuffix string
}

var _ xio.WriteHandler = (*ossHandler)(nil)

func NewOSSHandler(ctx context.Context, logger *slog.Logger, client *Client, enableOverwrite bool, opts ...xio.Option) xio.WriteHandler {
	w := xio.NewCommonWriteHandler(ctx, logger, opts...)

	// prepare oss handler
	h := &ossHandler{
		CommonWriteHandler: w,
		client:             client,
		transientSuffix:    "_inprogress",
	}

	// set appropriate schema and writer function
	h.SetSchema("oss")
	h.SetNewWriterFunc(func(destinationURI string) (io.Writer, error) {
		// remove object if it exists and overwrite is enabled
		if enableOverwrite {
			if err := h.client.Remove(destinationURI); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		// create a new writer with a transient suffix
		destinationURI = destinationURI + h.transientSuffix
		return client.NewWriter(destinationURI)
	})
	return h
}

func (h *ossHandler) Sync() error {
	// ensure all writes are flushed
	h.CommonWriteHandler.Sync()
	// move all files from transient URIs to final URIs
	for _, destinationURI := range h.DestinationURIs() {
		// copy the file from transient URI to final URI
		if err := h.client.Copy(destinationURI+h.transientSuffix, destinationURI); err != nil {
			return errors.WithStack(err)
		}
		// remove the transient file
		if err := h.client.Remove(destinationURI + h.transientSuffix); err != nil {
			return errors.WithStack(err)
		}
		h.Logger().Info(fmt.Sprintf("rename object to final uri %s is success", destinationURI))
	}
	return nil
}
