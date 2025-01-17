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

Transfer data from MaxCompute (MC) to Kafka and OSS:
```sh
./any2any --from=mc --to=kafka --to=oss
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
| SF | SF__HOST | Salesforce instance host. |
| | SF__USER | Salesforce username. |
| | SF__PASS | Salesforce password. |
| | SF__TOKEN | Salesforce security token. |
| | SF__SOQL_FILE_PATH | Path to the SOQL query file. (default: /data/in/main.soql)|
| | SF__COLUMN_MAPPING_FILE_PATH | Path to the mapping column for salesforce record result. (default: /data/in/mapping.columns) |
| GMAIL | GMAIL__TOKEN | Token JSON for gmail credentials |
| | GMAIL__FILTER | Gmail filter based on gmail filter rules |
| | GMAIL__EXTRACTOR_SOURCE | Which source to read (attachment, body) (default: attachment) |
| | GMAIL__EXTRACTOR_PATTERN | Pattern of the file to be downloaded (default: *) |
| | GMAIL__EXTRACTOR_FILE_FORMAT | Which format of file to be extracted (csv, json) (default: csv) |
| | GMAIL__FILENAME_COLUMN | Column name to retain filename of downloaded file. "" for ignore (default: "__FILENAME__") |
| | GMAIL__COLUMN_MAPPING_FILE_PATH | Path to the mapping column for gmail record result. "" for ignore (default: "") |
| MC | MC__SERVICE_ACCOUNT | Service account for MaxCompute. |
| | MC__QUERY_FILE_PATH | Path to the query file. (default: /data/in/query.sql) |
| | MC__EXECUTION_PROJECT | Project ID for the query execution. |

## Supported Sinks

| Component | Configuration | Description |
|---|---|---|
| MC | MC__SERVICE_ACCOUNT | Service account for MaxCompute. |
| | MC__DESTINATION_TABLE_ID | Destination table ID in Maxcompute. |
| | MC__LOAD_METHOD | Load method availability: APPEND, REPLACE. (default: APPEND) |
| | MC__UPLOAD_MODE | Upload mode availability: STREAM, REGULAR. (default: STREAM) |
| IO | - | - |
| OSS | OSS__SERVICE_ACCOUNT | Service account for OSS. |
| | OSS__DESTINATION_BUCKET_PATH | The destination path in a OSS bucket to put the result files. Must include the OSS bucket name. |
| | OSS__GROUP_BY | Available option: BATCH, COLUMN. "" for ignore |
| | OSS__GROUP_BATCH_SIZE | Batch size for the group by BATCH. |
| | OSS__GROUP_COLUMN_NAME | Column name for the group by COLUMN. |
| | OSS__COLUMN_MAPPING_FILE_PATH | Path to the mapping column for the record result. "" for ignore (default: "") |
| | OSS__FILENAME_PATTERN | Pattern to be used in the generated file names. eg `sample-{batch_start}-{batch_end}.json`. |
| | OSS__ENABLE_OVERWRITE | Flag to overwrite the file based on destination bucket path. |

## Supported Processors

| Component | Configuration | Description |
|---|---|---|
| JQ | JQ__QUERY | Any valid jq query. If set, it will override the query from file path. |
| | JQ__QUERY_FILE_PATH | Any valid jq query loaded from file. |
