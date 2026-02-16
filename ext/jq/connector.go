package jq

import (
	"bytes"
	"context"
	errs "errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strings"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
)

// NewJQConnectorExecFunc creates a ConnectorExecFunc that executes a jq command
// with the provided query on the inputReader. If the query is empty, it returns
// the inputReader without any processing.
func NewJQConnectorExecFunc(ctx context.Context, l *slog.Logger, query string) (common.ConnectorExecFunc, error) {
	tmpl, err := compiler.NewTemplate("connector_jq", string(query))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	compiledQuery, err := compiler.Compile(tmpl, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if _, err := exec.LookPath("jq"); err != nil {
		l.Error("processor: jq not found")
		return nil, errors.WithStack(err)
	}
	if strings.TrimSpace(compiledQuery) != "" {
		l.Info(fmt.Sprintf("creating jq connector exec func with query:\n%s", compiledQuery))
	}
	return func(inputReader io.Reader) (io.Reader, error) {
		if compiledQuery == "" {
			return inputReader, nil // no processing needed
		}

		// execute jq command with the provided query
		outputReader, err := execJQ(ctx, l, compiledQuery, inputReader)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		return outputReader, nil
	}, nil
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
		err := errors.WithStack(fmt.Errorf("jq error: %s", stderr.String()))
		l.Error(fmt.Sprintf("jq error: %s", err))
		e = errs.Join(e, err)
	}

	if e != nil {
		return nil, errors.WithStack(e)
	}

	return &stdout, nil
}
