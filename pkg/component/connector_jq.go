package component

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

const (
	batchSize = 512 // number of records to process in one batch
)

func execJQ(ctx context.Context, l *slog.Logger, query string, input []byte) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "jq", "-c", query)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		l.Error(fmt.Sprintf("jq error: %s", err))
		return nil, errors.WithStack(err)
	}

	if len(stderr.Bytes()) > 0 {
		err := fmt.Errorf("jq error: %s", stderr.String())
		l.Error(fmt.Sprintf("jq error: %s", err))
		return nil, errors.WithStack(err)
	}

	return bytes.TrimSpace(stdout.Bytes()), nil
}

func transformWithJQ(ctx context.Context, l *slog.Logger, query string, outlet flow.Outlet, inlets ...flow.Inlet) error {
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
			if err := flush(ctx, l, query, batchBuffer, inlets...); err != nil {
				return errors.WithStack(err)
			}
			// reset the buffer and counter
			batchBuffer.Reset()
			recordCount = 0
		}
	}

	// process any remaining records
	if recordCount > 0 {
		if err := flush(ctx, l, query, batchBuffer, inlets...); err != nil {
			return errors.WithStack(err)
		}
		// reset the buffer and counter
		batchBuffer.Reset()
		recordCount = 0
	}
	return nil
}

func processBatch(ctx context.Context, l *slog.Logger, query string, batchData []byte, inlet flow.Inlet) error {
	// transform the batch using JQ
	outputJSON, err := execJQ(ctx, l, query, batchData)
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

func flush(ctx context.Context, l *slog.Logger, query string, batchBuffer bytes.Buffer, inlets ...flow.Inlet) error {
	// store the record in a temporary buffer
	b := make([]byte, batchBuffer.Len())
	copy(b, batchBuffer.Bytes())

	for _, inlet := range inlets {
		// process the batch
		if err := processBatch(ctx, l, query, b, inlet); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}
