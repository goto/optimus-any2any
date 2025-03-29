package component

import (
	errs "errors"
	"fmt"
	"log/slog"
)

// Base is a component that provides minimal functionality.
// It is used as a base for other components.
type Base struct {
	l          *slog.Logger
	c          chan any
	err        error
	cleanFuncs []func() error
}

// NewBase creates a new base component.
func NewBase(l *slog.Logger, cleanFuncs ...func() error) *Base {
	b := &Base{
		l:          l,
		c:          make(chan any),
		err:        nil,
		cleanFuncs: make([]func() error, 0),
	}
	if len(cleanFuncs) > 0 {
		b.cleanFuncs = append(b.cleanFuncs, cleanFuncs...)
	}
	return b
}

// Logger returns the logger for logging.
func (b *Base) Logger() *slog.Logger {
	return b.l
}

// Close closes the component and runs all clean functions.
func (b *Base) Close() error {
	b.l.Debug("close")
	var e error
	for _, clean := range b.cleanFuncs {
		e = errs.Join(e, clean())
	}
	if e != nil {
		b.l.Warn(fmt.Sprintf("close error: %s", e.Error()))
	}
	return e
}

// Err returns the error if any.
func (b *Base) Err() error {
	return b.err
}
