package flow

// Inlet is an interface for a component that can receive data.
type Inlet interface {
	In() chan<- any
}

// Outlet is an interface for a component that can send data.
type Outlet interface {
	Out() <-chan any
}

// Source is an interface for source components.
// It contains Close method for closing the source.
type Source interface {
	Outlet
	Close()
	Err() error
}

// Sink is an interface for sink components.
// It contains Close method for closing the sink,
// and Wait method for waiting until the sink is done.
type Sink interface {
	Inlet
	Close()
	Wait()
	Err() error
}

// Connect is a function type that connects source and sink components.
type Connect func(Outlet, Inlet)

// ConnectMultiSink is a function type that connects source and multiple sink components.
type ConnectMultiSink func(Outlet, ...Inlet)
