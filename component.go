package main

import (
	"context"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// TODO: restructure the code to make it more modular and testable.

// TODO: add more source components.
func getSource(ctx context.Context, source string) flow.Source {
	return file.NewSource(ctx, "in.txt", flow.WithBufferSize(5))
}

// TODO: add more sink components.
func getSink(ctx context.Context, sink string) flow.Sink {
	return io.NewSink(ctx, flow.WithBufferSize(5))
}
