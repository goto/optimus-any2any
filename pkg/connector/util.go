package connector

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

const (
	workerDefaultCount = 1   // number of workers to process records. in future it can be configurable, for now it is 1 to keep the order of the records
	batchSize          = 512 // number of records to process in one batch
)

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

func transformWithJQ(l *slog.Logger, query string, outlet flow.Outlet, inlets ...flow.Inlet) {
	// create a buffer to hold the batch of records
	var batchBuffer bytes.Buffer
	recordCount := 0
	sem := make(chan uint8, workerDefaultCount)
	wg := &sync.WaitGroup{}

	// process records in batches
	for v := range outlet.Out() {
		// write the JSON byte array directly to the buffer
		batchBuffer.Write(append(v, '\n'))
		recordCount++

		// when we reach batch size, process the batch
		if recordCount >= batchSize {
			// store the record in a temporary buffer
			b := make([]byte, batchBuffer.Len())
			copy(b, batchBuffer.Bytes())

			for _, inlet := range inlets {
				// acquire a semaphore
				sem <- 0
				wg.Add(1)
				// spawn a goroutine to process the batch
				go func() {
					defer func() {
						<-sem     // release the semaphore
						wg.Done() // signal that the goroutine is done
					}()
					// process the batch
					processBatch(l, query, b, inlet)
				}()
			}

			// reset the buffer and counter
			batchBuffer.Reset()
			recordCount = 0
		}
	}

	// process any remaining records
	if recordCount > 0 {
		// store the record in a temporary buffer
		b := make([]byte, batchBuffer.Len())
		copy(b, batchBuffer.Bytes())

		for _, inlet := range inlets {
			// acquire a semaphore
			sem <- 0
			wg.Add(1)
			// spawn a goroutine to process the batch
			go func() {
				defer func() {
					<-sem     // release the semaphore
					wg.Done() // signal that the goroutine is done
				}()
				// process the batch
				processBatch(l, query, b, inlet)
			}()
		}
		// reset the buffer and counter
		batchBuffer.Reset()
		recordCount = 0
	}

	// wait for all goroutines to finish
	wg.Wait()
}

func processBatch(l *slog.Logger, query string, batchData []byte, inlet flow.Inlet) {
	l.Debug(fmt.Sprintf("connector(jq): processing batch of JSON records"))

	// transform the batch using JQ
	outputJSON, err := execJQ(l, query, batchData)
	if err != nil {
		l.Error(fmt.Sprintf("connector(jq): failed to transform JSON batch: %v", err))
		return
	}

	// split the result by newlines and send each record
	sc := bufio.NewScanner(bytes.NewReader(outputJSON))
	sc.Split(bufio.ScanLines)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)
		l.Debug(fmt.Sprintf("connector(jq): output JSON: %s", line))
		inlet.In(line)
	}

	l.Debug("connector(jq): batch processing complete")
}
