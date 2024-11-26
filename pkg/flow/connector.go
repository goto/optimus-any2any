package flow

// Connect is a function type that connects source and sink components.
type Connect func(Outlet, Inlet)
