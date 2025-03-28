package io

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
)

type IOSink struct {
	*common.Sink
	w io.Writer
}

var _ flow.Sink = (*IOSink)(nil)

func NewSink(l *slog.Logger, metadataPrefix string, opts ...common.Option) *IOSink {
	// create common
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	commonSink.SetName("%s")
	s := &IOSink{
		Sink: commonSink,
		w:    os.Stdout,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(io): close func called")
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s
}

func (s *IOSink) process() error {
	// read from channel
	for v := range s.Read() {
		s.Logger.Debug(fmt.Sprintf("%s: read: %s", s.Name(), string(v.([]byte))))
		fmt.Fprintf(s.w, "%s\n", string(v.([]byte)))
		s.Logger.Debug(fmt.Sprintf("%s: done: %s", s.Name(), string(v.([]byte))))
	}
	return nil
}
