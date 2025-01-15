package logger

import (
	"log/slog"

	"github.com/pkg/errors"
)

func NewLogger(logLevel string) (*slog.Logger, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return nil, errors.WithStack(err)
	}

	slog.SetLogLoggerLevel(level)
	return slog.Default(), nil
}

func NewDefaultLogger() *slog.Logger {
	l, _ := NewLogger("INFO")
	return l
}
