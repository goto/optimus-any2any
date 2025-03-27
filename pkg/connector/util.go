package connector

import (
	"bytes"
	"io"
	"log/slog"
	"os/exec"

	"github.com/pkg/errors"
)

func JQBinaryTransformation(l *slog.Logger, query string, input []byte) ([]byte, error) {
	// Prepare the jq command with more explicit quoting
	cmd := exec.Command("jq", "-c", query)

	// Create a pipe for input
	pipeReader, pipeWriter := io.Pipe()

	// Set up command to use pipe for input and output
	cmd.Stdin = pipeReader
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Error channel to capture any errors from goroutines
	errChan := make(chan error, 2)

	// Goroutine to write input to pipe
	go func() {
		defer pipeWriter.Close()
		_, err := pipeWriter.Write(input)
		errChan <- err
	}()

	// Goroutine to run jq command
	go func() {
		errChan <- cmd.Run()
	}()

	// Wait for input writing to complete
	if err := <-errChan; err != nil {
		return nil, errors.WithStack(err)
	}

	// Wait for command to complete
	cmdErr := <-errChan
	if cmdErr != nil {
		// If command failed, include stderr output for debugging
		return nil, errors.WithStack(cmdErr)
	}

	// Return the output, trimming any whitespace
	return bytes.TrimSpace(stdout.Bytes()), nil
}
