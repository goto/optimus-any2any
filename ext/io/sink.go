package io

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
)

type IOSink struct {
	*component.CoreSink
	w io.Writer
}

var _ flow.Sink = (*IOSink)(nil)

func NewSink(l *slog.Logger, metadataPrefix string) *IOSink {
	// create common
	coreSink := component.NewCoreSink(l, "io")
	s := &IOSink{
		CoreSink: coreSink,
		w:        os.Stdout,
	}

	// add clean func
	coreSink.AddCleanFunc(func() error {
		s.Logger().Debug("close func called")
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	coreSink.RegisterProcess(s.process)

	return s
}

func (s *IOSink) process() error {
	// read from channel
	for v := range s.Read() {
		s.Logger().Debug(fmt.Sprintf("read: %s", string(v.([]byte))))
		fmt.Fprintf(s.w, "%s\n", string(v.([]byte)))
		s.Logger().Debug(fmt.Sprintf("done: %s", string(v.([]byte))))
	}
	return nil
}
