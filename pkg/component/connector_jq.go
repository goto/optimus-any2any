package component

import (
	"bufio"
	"bytes"
	"context"
	errs "errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"

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

func transformWithJQ(ctx context.Context, l *slog.Logger, query string, metadataPrefix string, batchSize int, batchIndexColumn string, bufferSizeInMB int, outlet flow.Outlet, inlets ...flow.Inlet) error {
	// create a buffer to hold the batch of records
	var metadataBuffer bytes.Buffer
	var batchBuffer bytes.Buffer
	recordCount := 0

	// process records in batches
	for v := range outlet.Out() {
		if ok, err := isSpecializedMetadata(v, metadataPrefix); err != nil {
			return errors.WithStack(err)
		} else if ok {
			metadataBuffer.Write(append(v, '\n'))
			continue // skip specialized metadata records
		}

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
			if err := flush(ctx, l, bufferSizeInMB, query, &metadataBuffer, &batchBuffer, inlets...); err != nil {
				return errors.WithStack(err)
			}
			// reset the buffer
			metadataBuffer = bytes.Buffer{}
			batchBuffer = bytes.Buffer{}
		}
	}

	// process any remaining records
	if recordCount%batchSize != 0 {
		if err := flush(ctx, l, bufferSizeInMB, query, &metadataBuffer, &batchBuffer, inlets...); err != nil {
			return errors.WithStack(err)
		}
		// reset the buffer
		metadataBuffer = bytes.Buffer{}
		batchBuffer = bytes.Buffer{}
	}
	return nil
}

func processMetadata(_ context.Context, l *slog.Logger, metadataBytes []byte, inlet flow.Inlet) error {
	if len(metadataBytes) == 0 {
		l.Debug("no metadata to process")
		return nil
	}

	// split the metadata by newlines and send each record
	reader := bufio.NewReader(bytes.NewReader(metadataBytes))
	for {
		raw, err := reader.ReadBytes('\n')
		if len(raw) > 0 {
			line := make([]byte, len(raw))
			copy(line, raw)
			inlet.In(bytes.TrimSpace(line))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func processBatch(ctx context.Context, l *slog.Logger, _ int, query string, batchBytes []byte, inlet flow.Inlet) error {
	// transform the batch using JQ
	outputJSON, err := execJQ(ctx, l, query, batchBytes)
	if err != nil {
		l.Error(fmt.Sprintf("failed to transform JSON batch: %v", err))
		return errors.WithStack(err)
	}

	// split the result by newlines and send each record
	reader := bufio.NewReader(bytes.NewReader(outputJSON))
	for {
		raw, err := reader.ReadBytes('\n')
		if len(raw) > 0 {
			line := make([]byte, len(raw))
			copy(line, raw)
			inlet.In(bytes.TrimSpace(line))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func flush(ctx context.Context, l *slog.Logger, bufferSizeInMB int, query string, metadataBuffer, batchBuffer *bytes.Buffer, inlets ...flow.Inlet) error {
	for _, inlet := range inlets {
		// send metadata records to the inlet
		if err := processMetadata(ctx, l, metadataBuffer.Bytes(), inlet); err != nil {
			return errors.WithStack(err)
		}
		// process the batch
		if err := processBatch(ctx, l, bufferSizeInMB, query, batchBuffer.Bytes(), inlet); err != nil {
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

// TODO: this is closed to internal component, it should not be in pkg folder
func isSpecializedMetadata(v []byte, metadataPrefix string) (bool, error) {
	// Check if the record is a specialized metadata record
	var record orderedmapjson.AnyOrderedMap
	if err := json.Unmarshal(v, &record); err != nil {
		return false, errors.WithStack(err)
	}
	// Check if all of the keys in the record start with the metadata prefix
	for key := range record.AllFromFront() {
		if !strings.HasPrefix(key, metadataPrefix) {
			return false, nil
		}
	}
	return true, nil
}
