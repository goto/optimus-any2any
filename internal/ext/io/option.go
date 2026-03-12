package io

import (
	"fmt"

	"github.com/pkg/errors"
)

type Option func(w *chunkWriter) error

func WithChunkSize(size int) Option {
	return func(w *chunkWriter) error {
		if size <= 0 {
			return errors.WithStack(fmt.Errorf("chunk size must be greater than 0"))
		}
		w.chunkSize = size
		return nil
	}
}

func WithExtension(ext string) Option {
	return func(w *chunkWriter) error {
		if ext == "" {
			return errors.WithStack(fmt.Errorf("extension cannot be empty"))
		}
		w.extension = ext
		if ext == ".xlsx" {
			w.noChunk = true // xlsx format does not support chunking, file will be written in one go
		}
		return nil
	}
}

func WithCSVSkipHeader(skipHeader bool) Option {
	return func(w *chunkWriter) error {
		w.skipHeader = skipHeader
		return nil
	}
}

func WithCSVDelimiter(delimiter rune) Option {
	return func(w *chunkWriter) error {
		w.delimiter = delimiter
		return nil
	}
}
