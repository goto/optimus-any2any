package io

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"

	"github.com/pkg/errors"
)

// WriteHandler is an interface that defines methods for writing data to a destination URI.
type WriteHandler interface {
	// Write writes raw data to the destination URI.
	Write(destinationURI string, raw []byte) error
	// Sync flushes the data to the destination URI
	Sync() error
	// Close closes the handler and releases any resources.
	Close() error
	// DestinationURIs returns the list of destination URIs that this handler writes to.
	DestinationURIs() []string
	// Counters returns record count per destination uri
	Counters() map[string]int
}

type CommonWriteHandler struct {
	ctx           context.Context
	logger        *slog.Logger
	schema        string
	newWriter     func(string) (io.Writer, error)
	writers       map[string]WriteFlushCloser
	counters      map[string]int
	logCheckPoint int
	opts          []Option
}

var _ WriteHandler = (*CommonWriteHandler)(nil)

func NewCommonWriteHandler(ctx context.Context, logger *slog.Logger, opts ...Option) *CommonWriteHandler {
	return &CommonWriteHandler{
		ctx:    ctx,
		logger: logger,
		schema: "noschema",
		newWriter: func(destinationURI string) (io.Writer, error) {
			return nil, errors.New("newWriter function not implemented")
		},
		writers:       make(map[string]WriteFlushCloser),
		counters:      make(map[string]int),
		logCheckPoint: 1000,
		opts:          opts,
	}
}

func (h *CommonWriteHandler) Context() context.Context {
	return h.ctx
}

func (h *CommonWriteHandler) Logger() *slog.Logger {
	return h.logger
}

func (h *CommonWriteHandler) SetSchema(schema string) {
	h.schema = schema
}

func (h *CommonWriteHandler) SetNewWriterFunc(newWriter func(string) (io.Writer, error)) {
	h.newWriter = newWriter
}

func (h *CommonWriteHandler) Write(destinationURI string, raw []byte) error {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	if u.Scheme != h.schema {
		return errors.Errorf("invalid scheme: '%s', expected '%s'", u.Scheme, h.schema)
	}
	w, ok := h.writers[destinationURI]
	if !ok {
		writer, err := h.newWriter(destinationURI)
		if err != nil {
			return errors.WithStack(err)
		}
		ext := filepath.Ext(destinationURI)
		opts := append([]Option{WithExtension(ext)}, h.opts...)
		w = NewChunkWriter(h.logger, writer, opts...)
		h.writers[destinationURI] = w
		h.counters[destinationURI] = 0
	}

	_, err = w.Write(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	h.counters[destinationURI]++
	if h.counters[destinationURI]%h.logCheckPoint == 0 {
		h.logger.Info(fmt.Sprintf("eventually written %d records to file: %s", h.counters[destinationURI], destinationURI))
	}

	return nil
}

func (h *CommonWriteHandler) Sync() error {
	for _, writer := range h.writers {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (h *CommonWriteHandler) Close() error {
	for uri, writer := range h.writers {
		if err := writer.Close(); err != nil {
			return errors.WithStack(err)
		}
		h.logger.Info(fmt.Sprintf("closed writer for %s", uri))
		delete(h.writers, uri)
		delete(h.counters, uri)
	}
	return nil
}

func (h *CommonWriteHandler) DestinationURIs() []string {
	uris := make([]string, 0, len(h.writers))
	for uri := range h.writers {
		uris = append(uris, uri)
	}
	return uris
}

func (h *CommonWriteHandler) Counters() map[string]int {
	return h.counters
}
