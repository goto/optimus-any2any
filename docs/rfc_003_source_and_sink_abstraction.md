# [RFC] Source And Sink Abstraction

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 003 | [@deryrahman](https://github.com/deryrahman) | Accepted | 2024-12-19 | 2025-02-26 |

## Objective
Adding new source and sink components will require a lot of repetitive code. Abstraction could be used to reduce the repetitive code in the source and sink components. This RFC proposes a common component that can be shared across source and sink components. And provide common interface so that adding new source and sink components will be easier.

## Specification
Interface needed for source components:
- Send(data: any): void -> this method is used to send data to the channel internally
- RegisterProcess(process: Process): void -> this method is used to register a process function that will be executed when the source is created

Interface needed for sink components:
- Read(): any -> this method is used to receive data from the channel internally
- RegisterProcess(process: Process): void -> this method is used to register a process function that will be executed when the sink is created

Apart from the interface, common components need to abstracting the following parts:
- Channel creation + connection
- Channel management to avoid deadlock
- Metrics collection
- Error handling
- Logging

All of the common component should be configurable so that the user can customize the behavior according to their needs. Some configurable options that should be provided:
- LOG_LEVEL: string -> the log level that will be used by the logger
- BUFFER_SIZE: number -> the buffer size that will be used by the channel
- OTEL_<CONFIG>: any -> any other configuration that will be used by the OpenTelemetry
