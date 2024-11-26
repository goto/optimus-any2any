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
// It contains Options interface for setting options,
// and Close method for closing the source.
type Source interface {
	Outlet
	Options
	Close()
}

// Sink is an interface for sink components.
// It contains Options interface for setting options,
// Close method for closing the sink,
// and Wait method for waiting until the sink is done.
type Sink interface {
	Inlet
	Options
	Close()
	Wait()
}
