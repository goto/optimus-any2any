package jq

import (
	"bytes"
	"context"
	errs "errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
)

// NewJQConnectorExecFunc creates a ConnectorExecFunc that executes a jq command
// with the provided query on the inputReader. If the query is empty, it returns
// the inputReader without any processing.
func NewJQConnectorExecFunc(ctx context.Context, l *slog.Logger, query string) common.ConnectorExecFunc {
	l.Info(fmt.Sprintf("creating jq connector exec func with query:\n%s", query))
	return func(inputReader io.Reader) (io.Reader, error) {
		if query == "" {
			return inputReader, nil // no processing needed
		}

		// execute jq command with the provided query
		outputReader, err := execJQ(ctx, l, query, inputReader)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		return outputReader, nil
	}
}

func execJQ(ctx context.Context, l *slog.Logger, query string, inputReader io.Reader) (io.Reader, error) {
	cmd := exec.CommandContext(ctx, "jq", "-c", query)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdin = inputReader
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

	return &stdout, nil
}
