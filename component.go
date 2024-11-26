package main

import (
	"log/slog"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// TODO: restructure the code to make it more modular and testable.

// TODO: add more source components.
func getSource(l *slog.Logger, source string) flow.Source {
	fs, _ := file.NewSource(l, "in.txt", flow.WithBufferSize(5))
	return fs
}

// TODO: add more sink components.
func getSink(l *slog.Logger, sink string) flow.Sink {
	return io.NewSink(l, flow.WithBufferSize(5))
}
