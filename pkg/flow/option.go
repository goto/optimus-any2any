package flow

// Options is an interface for setting options.
// It is implemented for internal use.
type Options interface {
	SetBufferSize(size int)
}

// Option is a function type for setting options.
type Option func(Options)

// WithBufferSize is an option for setting channel buffer size.
func WithBufferSize(size int) Option {
	return func(o Options) {
		o.SetBufferSize(size)
	}
}
