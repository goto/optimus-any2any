package io

import (
	"bufio"
	"io"
)

type BufferReader struct {
	*bufio.Reader
}

// NewBufferReader creates a new buffered reader.
func NewBufferReader(r io.Reader) *BufferReader {
	return &BufferReader{
		Reader: bufio.NewReaderSize(r, 32*1024),
	}
}

// Close do nothing
func (b *BufferReader) Close() error {
	return nil
}
