# optimus-any2any

## Overview
`optimus-any2any` is a versatile tool designed to transfer data from any source to any sink with configurable options. It supports various data sources and sinks, providing a flexible and powerful way to handle data transfers.

## Features
- Transfer data between multiple sources and sinks.
- Configurable through command-line options.
- Supports environment variable configurations.
- High performance and reliability.

## Installation
To install the tool, download the binary from the releases page and make it executable:

```sh
chmod +x any2any
```

## Usage
Here are some examples of how to use optimus-any2any:

### Basic Usage
Transfer data from a file to MaxCompute (MC):
```sh
./any2any --from=file --to=mc
```

Transfer data from MaxCompute (MC) to Kafka:
```sh
./any2any --from=mc --to=kafka
```

It expects configuration from env variables. Or you can pass configuration from arguments env directly:
```sh
./any2any --from=file --to=mc \
--env="FILE__PATH=./in.txt" \
--env="MC__SERVICE_ACCOUNT=svc_account" \
--env="MC__DESTINATION_TABLE_ID=project.sample.table"
```

### Configuration convention
`<source/sink>__<config_name>`, example:
- FILE__PATH: Path to the input file.
- MC__SERVICE_ACCOUNT: Service account for MaxCompute.
- MC__DESTINATION_TABLE_ID: Destination table ID in MaxCompute.


## Supported Sources

| Component | Configuration | Description |
|---|---|---|
| FILE | FILE__PATH | Path to the input file. |

## Supported Sinks

| Component | Configuration | Description |
|---|---|---|
| MC | MC__SERVICE_ACCOUNT | Service account for MaxCompute. |
| | MC__DESTINATION_TABLE_ID | Destination table ID in Maxcompute. |
| IO | - | - |

## Supported Processors

| Component | Configuration | Description |
|---|---|---|
| JQ | JQ__QUERY | Any valid jq query. |
