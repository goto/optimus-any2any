package connector

import (
	"bytes"
	"io"
	"log/slog"
	"os/exec"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/klauspost/readahead"
	"github.com/pkg/errors"
)

func JQBinaryTransformation(l *slog.Logger, query string, input []byte) ([]byte, error) {
	buf := buffer.New(32 * 1024) // 32KB buffer
	rp, w := nio.Pipe(buf)
	r, err := readahead.NewReaderSize(rp, readahead.DefaultBuffers, int(buf.Cap()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cmd := exec.Command("jq", "-c", query)

	cmd.Stdin = r
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	errChan := make(chan error, 2)

	go func(w io.WriteCloser) {
		defer w.Close()
		_, err := w.Write(input)
		errChan <- err
	}(w)

	go func() {
		errChan <- cmd.Run()
	}()

	if err := <-errChan; err != nil {
		return nil, errors.WithStack(err)
	}

	if err := <-errChan; err != nil {
		return nil, errors.WithStack(err)
	}

	return bytes.TrimSpace(stdout.Bytes()), nil
}
