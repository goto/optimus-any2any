package component

import (
	"bufio"
	errs "errors"
	"fmt"
	"io"
	"iter"
	"log/slog"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/klauspost/readahead"
	"github.com/pkg/errors"
)

const (
	backendChannel = "channel"
	backendIO      = "io"
)

// Registrants is an interface that defines the methods
// that a component must implement to register processes and clean functions.
type Registrants interface {
	AddCleanFunc(f func() error)
	RegisterProcess(f func() error)
}

// Setter is an interface that defines the methods
// that a component must implement to set some internal
// properties like logger and buffer size.
type Setter interface {
	SetLogger(l *slog.Logger)
	SetBackend(backend string)
	SetBufferSize(size int)
}

// Core is a struct that implements the Registrants and Setter interfaces.
type Core struct {
	*Base
	backend         string
	size            int
	c               chan []byte    // backend: channel
	w               io.WriteCloser // backend: io
	r               io.ReadCloser  // backend: io
	component       string
	name            string
	postHookProcess func() error // this is called after all processes are done
}

var _ Registrants = (*Core)(nil)
var _ Setter = (*Core)(nil)
var _ flow.Inlet = (*Core)(nil)
var _ flow.Outlet = (*Core)(nil)

// NewCore creates a new Core instance.
func NewCore(l *slog.Logger, component, name string) *Core {
	c := &Core{
		Base:            NewBase(l),
		backend:         backendChannel,
		size:            0,
		component:       component,
		name:            name,
		postHookProcess: func() error { return nil },
	}
	c.initBackend()
	c.l = c.l.WithGroup(c.component).With("name", c.name)
	return c
}

// SetLogger sets the logger for the core component.
func (c *Core) SetLogger(l *slog.Logger) {
	c.l = l
	c.l = c.l.WithGroup(c.component).With("name", c.name)
}

// SetBufferSize sets the buffer size for the channel.
func (c *Core) SetBufferSize(size int) {
	c.size = size
	c.initBackend()
}

// SetBackend sets the backend for the core component.
func (c *Core) SetBackend(backend string) {
	c.backend = backend
	c.initBackend()
}

// Component returns the component type of the core.
func (c *Core) Component() string {
	return c.component
}

// Name returns the name of the core component.
func (c *Core) Name() string {
	return c.name
}

// AddCleanFunc adds a clean function to the component.
// Clean functions are called when the component is closed
// whether it is closed gracefully or due to an error.
func (c *Core) AddCleanFunc(f func() error) {
	c.cleanFuncs = append(c.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function is expected to return an error if it fails.
// And postHookProcess is called after all processes are done.
func (c *Core) RegisterProcess(f func() error) {
	go func() {
		defer func() {
			c.postHookProcess()
		}()
		if err := f(); err != nil {
			c.l.Error(fmt.Sprintf("process error: %s", err.Error()))
			c.err = errors.WithStack(err)
		}
	}()
}

// In receives a value to the channel.
func (c *Core) In(v []byte) {
	switch c.backend {
	case backendChannel:
		c.inChannel(v)
	case backendIO:
		c.inIO(v)
	default:
		c.l.Warn(fmt.Sprintf("unknown backend: %s; using channel", c.backend))
		c.inChannel(v)
	}
}

// CloseInlet closes the inlet.
func (c *Core) CloseInlet() error {
	switch c.backend {
	case backendChannel:
		return c.closeInletChannel()
	case backendIO:
		return c.closeInletIO()
	default:
		c.l.Warn(fmt.Sprintf("unknown backend: %s; using channel", c.backend))
		return c.closeInletChannel()
	}
}

// Out returns iterator for the channel.
func (c *Core) Out() iter.Seq[[]byte] {
	switch c.backend {
	case backendChannel:
		return c.outChannel()
	case backendIO:
		return c.outIO()
	default:
		c.l.Warn(fmt.Sprintf("unknown backend: %s; using channel", c.backend))
		return c.outChannel()
	}
}

func (c *Core) inChannel(v []byte) {
	c.c <- v
}

func (c *Core) inIO(v []byte) {
	_, err := c.w.Write(append(v, '\n'))
	if err != nil && !errs.Is(err, io.ErrClosedPipe) {
		c.l.Warn(fmt.Sprintf("failed to write to sink: %s", err.Error()))
	}
}

func (c *Core) closeInletChannel() error {
	close(c.c)
	return nil
}

func (c *Core) closeInletIO() error {
	return c.w.Close()
}

func (c *Core) outChannel() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for v := range c.c {
			if !yield(v) {
				break
			}
		}
	}
}

func (c *Core) outIO() iter.Seq[[]byte] {
	sc := bufio.NewScanner(c.r)
	// return a function that takes a yield function
	return func(yield func([]byte) bool) {
		for sc.Scan() {
			raw := sc.Bytes()
			line := make([]byte, len(raw))
			copy(line, raw)
			if !yield(line) {
				break
			}
		}
	}
}

func (c *Core) initBackend() {
	switch c.backend {
	case backendChannel:
		c.initBackendChannel()
	case backendIO:
		c.initBackendIO()
	default:
		c.l.Warn(fmt.Sprintf("unknown backend: %s; using channel", c.backend))
		c.initBackendChannel()
	}
}

func (c *Core) initBackendChannel() {
	c.c = make(chan []byte)
	if c.size > 0 {
		c.c = make(chan []byte, c.size)
	}
	// clear io
	c.w = nil
	c.r = nil
}

func (c *Core) initBackendIO() {
	buf := buffer.New(readahead.DefaultBufferSize)
	rp, w := nio.Pipe(buf)

	r := readahead.NewReader(rp)
	if c.size > 0 {
		reader, err := readahead.NewReaderSize(rp, c.size, readahead.DefaultBufferSize)
		if err != nil {
			c.l.Warn(fmt.Sprintf("failed to set buffer size; %s; use default", err.Error()))
		} else {
			r = reader
		}
	}

	c.w = w
	c.r = r
	// clear channel
	c.c = nil
}
