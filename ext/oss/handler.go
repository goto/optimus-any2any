package oss

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/ext/fs"
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
		if err := client.Remove(transientDestinationURI); err != nil { // make sure transient file does not exist
			return nil, errors.WithStack(err)
		}
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
	tasks := []func() error{}
	for _, destinationURI := range h.DestinationURIs() {
		destinationURICopy := destinationURI // capture range variable
		tasks = append(tasks, func() error {
			// copy the file from transient URI to final URI
			h.Logger().Info(fmt.Sprintf("renaming object from transient uri %s to final uri %s...", fs.MaskedURI(destinationURICopy+DefaultTransientSuffix), fs.MaskedURI(destinationURICopy)))
			if err := h.client.Copy(destinationURICopy+DefaultTransientSuffix, destinationURICopy); err != nil {
				return errors.WithStack(err)
			}
			// remove the transient file
			h.Logger().Info(fmt.Sprintf("removing transient object %s...", fs.MaskedURI(destinationURICopy+DefaultTransientSuffix)))
			if err := h.client.Remove(destinationURICopy + DefaultTransientSuffix); err != nil {
				return errors.WithStack(err)
			}
			h.Logger().Info(fmt.Sprintf("rename object to final uri %s is success", fs.MaskedURI(destinationURICopy)))
			return nil
		})
	}
	return errors.WithStack(h.ConcurrentTasks(tasks)) // wait for all copy operations to finish
}
