package component

import (
	"fmt"
	"log/slog"
)

// CoreSource is a helper struct that supports the source interface.
// To comply with the source interface, it must be declared in tandem with Core.
// Eg.
// ```go
//
//	type MySource struct {
//		*component.CoreSource
//		*component.Core
//	}
//
// ```
// MySource will be guaranteed to implement the source interface
type CoreSource struct {
	Core *Core
}

// NewCoreSource creates a new CoreSource instance.
func NewCoreSource(l *slog.Logger, name string) *CoreSource {
	c := &CoreSource{
		Core: NewCore(l, "source", name),
	}
	// special case for source to close the channel
	// after all processes are done
	c.Core.postHookProcess = func() error {
		close(c.Core.c)
		c.Core.l.Debug(fmt.Sprintf("close success"))
		return nil
	}
	return c
}

// Component returns the component type of the source
func (c *CoreSource) Component() string {
	return "source"
}

// Out returns the channel to read from
func (c *CoreSource) Out() <-chan any {
	return c.Core.c
}

// Send sends a value to the channel
func (c *CoreSource) Send(v any) {
	c.Core.c <- v
}
