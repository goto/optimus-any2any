package py

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os/exec"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
)

// NewPYConnectorExecFunc creates a ConnectorExecFunc that executes a Python script
// on the input reader. The Python script receives newline-delimited JSON on stdin
// and must output newline-delimited JSON on stdout.
func NewPYConnectorExecFunc(ctx context.Context, l *slog.Logger, scriptPath string, pythonBinary string) (common.ConnectorExecFunc, error) {
	if scriptPath == "" {
		return nil, errors.New("python script path is required for PY processor")
	}

	if _, err := exec.LookPath(pythonBinary); err != nil {
		l.Error("processor: python binary not found")
		return nil, errors.WithStack(err)
	}

	l.Info("creating PY connector with script: " + scriptPath)

	return func(inputReader io.Reader) (io.Reader, error) {
		cmd := exec.CommandContext(ctx, pythonBinary, scriptPath)

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdin = inputReader
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			l.Error("python script error: " + err.Error())
			if stderr.Len() > 0 {
				l.Error("python stderr: " + stderr.String())
			}
			return nil, errors.WithStack(err)
		}

		if stderr.Len() > 0 {
			l.Warn("python stderr: " + stderr.String())
		}

		return &stdout, nil
	}, nil
}
