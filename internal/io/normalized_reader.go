package io

import (
	"io"

	"github.com/pkg/errors"
)

type NormalizeLineEndingReader struct {
	io.Reader
	buf [1]byte
}

func NewNormalizeLineEndingReader(r io.Reader) *NormalizeLineEndingReader {
	return &NormalizeLineEndingReader{Reader: r}
}

func (n *NormalizeLineEndingReader) Read(p []byte) (int, error) {
	i := 0
	for i < len(p) {
		b, err := n.readNormalizedByte()
		if err != nil {
			return i, errors.WithStack(err)
		}
		p[i] = b
		i++
	}
	return i, nil
}

func (n *NormalizeLineEndingReader) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := n.Reader.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	return 0, nil
}

func (n *NormalizeLineEndingReader) readNormalizedByte() (byte, error) {
	nRead, err := n.Reader.Read(n.buf[:])
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if nRead == 0 {
		return 0, io.EOF
	}
	b := n.buf[0]

	if b == '\r' {
		return '\n', nil
	}

	return b, nil
}
