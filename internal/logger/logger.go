package logger

import (
	"log/slog"
	"os"

	"github.com/pkg/errors"
)

func NewLogger(logLevel string) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return nil, errors.WithStack(err)
	}

	writter := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(writter), nil
}

func NewDefaultLogger() *slog.Logger {
	l, _ := NewLogger("INFO")
	return l
}
