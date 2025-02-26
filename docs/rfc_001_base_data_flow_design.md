# [RFC] Base Data Flow Design

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 001 | @deryrahman | Accepted | 2024-11-04 | 2025-02-26 |

## Objective
Existing plugins are not flexible enough to handle different data sources and data sinks. This RFC proposes a new data flow design that is more flexible and can handle different data sources and data sinks.

## Approach
The new data flow design will consist of three main components: data sources, data processors, and data sinks. Data sources will be responsible for reading data from different sources, data processors will be responsible for processing the data, and data sinks will be responsible for writing the data to different sinks.

With this approach data flow process can be easily customized by combining different data sources and data sinks.

### Go Interface Design (Source, Sink, Connector)
To support pluggable data sources, data processors, and data sinks, we will define the following Go interfaces:

```go
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
```

It's subject to change based on the implementation. But the main idea is to have a common interface for data sources, data processors, and data sinks. Any implementation that satisfies the interface can be used in the data flow process.

### Pipeline Implementation
The pipeline will be implemented as a chain of data sources and data sinks. Each data source will read data from a source and send it through the golang channel. Each data sink will receive data from the golang channel and write it to a sink.

### Data Format Agreement
To make sure that data sources, data processors, and data sinks can work together, we need to define a common data format. Some data format such as JSON, CSV, Avro, Parquet, Protobuf, Arrow, etc. might be used, but for simplicity, we will use JSON as the common data format.
