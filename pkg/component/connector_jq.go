package component

import (
	"bufio"
	"bytes"
	"context"
	errs "errors"
	"fmt"
	"log/slog"
	"os/exec"

	"github.com/GitRowin/orderedmapjson"
	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

func execJQ(ctx context.Context, l *slog.Logger, query string, input []byte) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "jq", "-c", query)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	var e error
	if err := cmd.Run(); err != nil {
		l.Error(fmt.Sprintf("jq error: %s", err))
		e = errs.Join(e, err)
	}

	if len(stderr.Bytes()) > 0 {
		err := fmt.Errorf("jq error: %s", stderr.String())
		l.Error(fmt.Sprintf("jq error: %s", err))
		e = errs.Join(e, err)
	}

	if e != nil {
		return nil, errors.WithStack(e)
	}

	return bytes.TrimSpace(stdout.Bytes()), nil
}

func transformWithJQ(ctx context.Context, l *slog.Logger, query string, batchSize int, batchIndexColumn string, outlet flow.Outlet, inlets ...flow.Inlet) error {
	// create a buffer to hold the batch of records
	var batchBuffer bytes.Buffer
	recordCount := 0

	// process records in batches
	for v := range outlet.Out() {
		// add batch index as metadata
		raw, err := addBatchIndex(v, batchIndexColumn, int(recordCount/batchSize))
		if err != nil {
			l.Error(fmt.Sprintf("failed to add batch index: %v", err))
			return errors.WithStack(err)
		}
		// write the JSON byte array directly to the buffer
		batchBuffer.Write(append(raw, '\n'))
		recordCount++

		// when we reach batch size, process the batch
		if recordCount%batchSize == 0 {
			if err := flush(ctx, l, query, batchBuffer, inlets...); err != nil {
				return errors.WithStack(err)
			}
			// reset the buffer
			batchBuffer.Reset()
		}
	}

	// process any remaining records
	if recordCount%batchSize != 0 {
		if err := flush(ctx, l, query, batchBuffer, inlets...); err != nil {
			return errors.WithStack(err)
		}
		// reset the buffer
		batchBuffer.Reset()
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
	buf := make([]byte, 0, 4*1024)
	sc.Buffer(buf, 1024*1024)

	sc.Split(bufio.ScanLines)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)
		inlet.In(line)
	}
	if err := sc.Err(); err != nil {
		l.Error(fmt.Sprintf("failed to read transformed JSON: %v", err))
		return errors.WithStack(err)
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

// TODO: this is closed to internal component, it should not be in pkg folder
func addBatchIndex(v []byte, batchIndexColumn string, batchIndex int) ([]byte, error) {
	var record orderedmapjson.AnyOrderedMap
	if err := json.Unmarshal(v, &record); err != nil {
		return nil, errors.WithStack(err)
	}
	if batchIndexColumn != "" {
		record.Set(batchIndexColumn, batchIndex)
	}
	// marshal the record back to JSON
	marshaled, err := json.Marshal(record)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return marshaled, nil
}
