package component

import (
	"context"
	errs "errors"
	"fmt"
	"log/slog"

	"github.com/pkg/errors"
)

// Registrants is an interface that defines the methods
// that a component must implement to register processes and clean functions.
type Registrants interface {
	AddCleanFunc(f func() error)
	RegisterProcess(f func() error)
}

// Base is a component that provides minimal functionality.
// It is used as a base for other components.
type Base struct {
	ctx             context.Context
	cancelFn        context.CancelCauseFunc
	l               *slog.Logger
	err             error
	cleanFuncs      []func() error
	postHookProcess func() error // this is called after all processes are done but before closing
	done            chan uint8
}

var _ Registrants = (*Base)(nil)

// NewBase creates a new base component.
func NewBase(ctx context.Context, cleanFn context.CancelCauseFunc, l *slog.Logger) *Base {
	b := &Base{
		ctx:             ctx,
		cancelFn:        cleanFn,
		l:               l,
		err:             nil,
		cleanFuncs:      make([]func() error, 0),
		postHookProcess: func() error { return nil },
		done:            make(chan uint8),
	}
	return b
}

// Logger returns the logger for logging.
func (b *Base) Logger() *slog.Logger {
	return b.l
}

// Close closes the component and runs all clean functions.
func (b *Base) Close() error {
	<-b.done

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

// AddCleanFunc adds a clean function to the component.
// Clean functions are called when the component is closed
// whether it is closed gracefully or due to an error.
func (b *Base) AddCleanFunc(f func() error) {
	b.cleanFuncs = append(b.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function is expected to return an error if it fails.
// And postHookProcess is called after all processes are done.
func (b *Base) RegisterProcess(f func() error) {
	go func() {
		defer func() {
			b.postHookProcess()
			b.done <- 0
		}()

		// wait until the context is canceled or the process is done
		select {
		case <-b.ctx.Done():
			msg := "context canceled"
			if err := context.Cause(b.ctx); err != nil {
				msg = fmt.Sprintf("%s: %s", msg, err.Error())
			}
			b.l.Info(msg)
		case <-process(f, &b.err):
			b.l.Info("process done")
			if b.err != nil {
				b.l.Error(fmt.Sprintf("process error: %s", b.err.Error()))
				b.cancelFn(b.err)
			}
		}
	}()
}

func process(f func() error, e *error) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := f()
		if err != nil {
			ee := errors.WithStack(err)
			*e = ee
		}
	}()
	return done
}
