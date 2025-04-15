package connector

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os/exec"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

const (
	batchSize = 512 // number of records to process in one batch
)

func passthrough(_ *slog.Logger, outlet flow.Outlet, inlets ...flow.Inlet) error {
	for v := range outlet.Out() {
		for _, inlet := range inlets {
			inlet.In(v)
		}
	}
	return nil
}

func execJQ(_ *slog.Logger, query string, input []byte) ([]byte, error) {
	buf := buffer.New(32 * 1024)
	r, w := nio.Pipe(buf)

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

func transformWithJQ(l *slog.Logger, query string, outlet flow.Outlet, inlets ...flow.Inlet) error {
	// create a buffer to hold the batch of records
	var batchBuffer bytes.Buffer
	recordCount := 0

	// process records in batches
	for v := range outlet.Out() {
		// write the JSON byte array directly to the buffer
		batchBuffer.Write(append(v, '\n'))
		recordCount++

		// when we reach batch size, process the batch
		if recordCount >= batchSize {
			if err := flush(l, query, batchBuffer, inlets...); err != nil {
				return errors.WithStack(err)
			}
			// reset the buffer and counter
			batchBuffer.Reset()
			recordCount = 0
		}
	}

	// process any remaining records
	if recordCount > 0 {
		if err := flush(l, query, batchBuffer, inlets...); err != nil {
			return errors.WithStack(err)
		}
		// reset the buffer and counter
		batchBuffer.Reset()
		recordCount = 0
	}
	return nil
}

func processBatch(l *slog.Logger, query string, batchData []byte, inlet flow.Inlet) error {
	// transform the batch using JQ
	outputJSON, err := execJQ(l, query, batchData)
	if err != nil {
		l.Error(fmt.Sprintf("failed to transform JSON batch: %v", err))
		return errors.WithStack(err)
	}

	// split the result by newlines and send each record
	sc := bufio.NewScanner(bytes.NewReader(outputJSON))
	sc.Split(bufio.ScanLines)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)
		inlet.In(line)
	}

	return nil
}

func flush(l *slog.Logger, query string, batchBuffer bytes.Buffer, inlets ...flow.Inlet) error {
	// store the record in a temporary buffer
	b := make([]byte, batchBuffer.Len())
	copy(b, batchBuffer.Bytes())

	for _, inlet := range inlets {
		// process the batch
		if err := processBatch(l, query, b, inlet); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}
