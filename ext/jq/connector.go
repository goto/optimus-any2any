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
	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/goto/optimus-any2any/internal/model"
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
	l.Info(fmt.Sprintf("creating jq connector exec func with query:\n%s", query))
	return func(inputReader io.Reader) (io.Reader, error) {
		if query == "" {
			return inputReader, nil // no processing needed
		}

		if strings.TrimSpace(compiledQuery) == strings.TrimSpace(query) {
			l.Info("using raw query without compilation")

			// execute jq command with the provided query
			outputReader, err := execJQ(ctx, l, query, inputReader)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			return outputReader, nil
		}

		l.Info("using compiled query")
		compiledQueries := []string{}
		inputBuffers := []bytes.Buffer{}
		for record, err := range helper.NewRecordReader(l, inputReader).ReadRecord() {
			if err != nil {
				return nil, errors.WithStack(err)
			}

			// compile the query with the record
			currentCompiledQuery, err := compiler.Compile(tmpl, model.ToMap(record))
			if err != nil {
				return nil, errors.WithStack(err)
			}
			currentCompiledQuery = strings.TrimSpace(currentCompiledQuery)

			// group records with the same compiled query
			if len(compiledQueries) == 0 || compiledQueries[len(compiledQueries)-1] != currentCompiledQuery {
				compiledQueries = append(compiledQueries, currentCompiledQuery)
				inputBuffers = append(inputBuffers, bytes.Buffer{})
			}
			raw, err := record.MarshalJSON()
			if err != nil {
				return nil, errors.WithStack(err)
			}
			inputBuffers[len(inputBuffers)-1].Write(raw)
			inputBuffers[len(inputBuffers)-1].WriteByte('\n')
		}

		outputBuffer := bytes.Buffer{}
		for i, currentCompiledQuery := range compiledQueries {
			l.Debug(fmt.Sprintf("executing jq with compiled query:\n%s", currentCompiledQuery))
			outputReader, err := execJQ(ctx, l, currentCompiledQuery, &inputBuffers[i])
			if err != nil {
				return nil, errors.WithStack(err)
			}
			_, err = io.Copy(&outputBuffer, outputReader)
			if err != nil {
				return nil, errors.WithStack(err)
			}
		}

		return &outputBuffer, nil
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
		err := fmt.Errorf("jq error: %s", stderr.String())
		l.Error(fmt.Sprintf("jq error: %s", err))
		e = errs.Join(e, err)
	}

	if e != nil {
		return nil, errors.WithStack(e)
	}

	return &stdout, nil
}
