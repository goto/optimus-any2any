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
--env="MC__CREDENTIALS='{creds:1}'" \
--env="MC__DESTINATION_TABLE_ID=project.sample.table"
```

### Configuration convention
`<source/sink>__<config_name>`, example:
- FILE__PATH: Path to the input file.
- MC__CREDENTIALS: Credentials for MaxCompute.
- MC__DESTINATION_TABLE_ID: Destination table ID in MaxCompute.


### Advanced Usage
**Use JQ processor to filter or transform data before transferring:**

You can use the JQ processor to filter or transform data before transferring it to the sink. The JQ processor allows you to apply any valid jq query to the data. It's a default processor that can be used with any source or sink.

```sh
./any2any --from=file --to=mc \
--env="FILE__PATH=./in.txt" \
--env="MC__CREDENTIALS='{creds:1}'" \
--env="MC__DESTINATION_TABLE_ID=project.sample.table" \
--env="JQ__QUERY=.[] | select(.age > 30)"
```

**Use direct execution without data transfer:**

It applies when sink and source are in the same environment. For example, transferring data from OSS to MaxCompute table. Use the `--no-pipeline` flag to execute the source and sink directly without transferring data.

```sh
./any2any --from=oss --to=mc --no-pipeline \
--env="OSS2MC__SOURCE_URI=oss://bucket/path" \
--env="OSS2MC__CREDENTIALS='{creds:1}'" \
--env="OSS2MC__DESTINATION_TABLE_ID=project.sample.table"
```


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
| MC | MC__CREDENTIALS | Credentials for MaxCompute. |
| | MC__QUERY_FILE_PATH | Path to the query file. (default: /data/in/query.sql) |
| | MC__EXECUTION_PROJECT | Project ID for the query execution. |
| OSS | OSS__CREDENTIALS | Credentials for OSS. |
| | OSS__SOURCE_URI | The source path in a OSS bucket to read the files. Format oss://bucket/path/to/folder. |
| | OSS__FILE_FORMAT | File format availability: CSV, JSON. (default: JSON) |
| | OSS__CSV_DELIMITER | Delimiter for CSV file format. (default: ,) |
| | OSS__COLUMN_MAPPING_FILE_PATH | Path to the mapping column for the record result. "" for ignore (default: "") |
## Supported Sinks

| Component | Configuration | Description |
|---|---|---|
| FILE | FILE__DESTINATION_URI | Path to the output file. Format file:///directory/to/somthing.extension |
| MC | MC__CREDENTIALS | Credentials for MaxCompute. |
| | MC__DESTINATION_TABLE_ID | Destination table ID in Maxcompute. |
| | MC__LOAD_METHOD | Load method availability: APPEND, REPLACE. (default: APPEND) |
| | MC__UPLOAD_MODE | Upload mode availability: STREAM, REGULAR. (default: STREAM) |
| IO | - | - |
| OSS | OSS__CREDENTIALS | Credentials for OSS. |
| | OSS__DESTINATION_URI | The destination path in a OSS bucket to put the result files. Format oss://bucket/path/to/folder |
| | OSS__GROUP_BY | Available option: BATCH, COLUMN. "" for ignore |
| | OSS__GROUP_BATCH_SIZE | Batch size for the group by BATCH. |
| | OSS__GROUP_COLUMN_NAME | Column name for the group by COLUMN. |
| | OSS__COLUMN_MAPPING_FILE_PATH | Path to the mapping column for the record result. "" for ignore (default: "") |
| | OSS__FILENAME_PATTERN | Pattern to be used in the generated file names. eg `sample-{batch_start}-{batch_end}.json`. |
| | OSS__ENABLE_OVERWRITE | Flag to overwrite the file based on destination bucket path. |
| SFTP | SFTP__ADDRESS | SFTP server address, with format `host:port`. |
| | SFTP__USERNAME | SFTP username for authentication. |
| | SFTP__PRIVATE_KEY | SFTP private key for authentication. "" for ignore |
| | SFTP__PASSWORD | SFTP password for authentication. "" for ignore |
| | SFTP__HOST_FINGERPRINT | SFTP host fingerprint for authentication. "" for ignore |
| | SFTP__GROUP_BY | Available option: BATCH, COLUMN. "" for ignore |
| | SFTP__GROUP_BATCH_SIZE | Batch size for the group by BATCH. |
| | SFTP__GROUP_COLUMN_NAME | Column name for the group by COLUMN. |
| | SFTP__COLUMN_MAPPING_FILE_PATH | Path to the mapping column for the record result. "" for ignore (default: "") |
| | SFTP__FILE_FORMAT | File format availability: CSV, TSV, JSON. (default: JSON) |
| | SFTP__DESTINATION_PATH | The destination path in a SFTP server to put the result files. |
| | SFTP__FILENAME_PATTERN | Pattern to be used in the generated file names. eg `sample-{batch_start}-{batch_end}.json`. |
| KAFKA | KAFKA__BOOTSTRAP_SERVERS | Kafka bootstrap servers, comma-separated. |
| | KAFKA__TOPIC | Kafka topic to write the data. |

## Supported Processors

| Component | Configuration | Description |
|---|---|---|
| JQ | JQ__QUERY | Any valid jq query. If set, it will override the query from file path. |
| | JQ__QUERY_FILE_PATH | Any valid jq query loaded from file. |

## Supported Direct Execution For Data Transfer
| Component | Configuration | Description |
|---|---|---|
| OSS2MC | OSS2MC__CREDENTIALS | Credentials for MaxCompute. |
| | OSS2MC__SOURCE_URI | The source path in a OSS bucket to read the files. Format oss://bucket/path/to/folder |
| | OSS2MC__DESTINATION_TABLE_ID | Destination table ID in Maxcompute. |
| | OSS2MC__FILE_FORMAT | File format availability: CSV, JSON. (default: JSON) |
| | OSS2MC__LOAD_METHOD | Load method availability: APPEND, REPLACE. (default: APPEND) |
| | OSS2MC__PARTITION_VALUES | Partition values for the destination table. Format partitionkey1=value1,partitionkey2=value2. "" for ignore (default: "") |
| | OSS2MC__LOG_VIEW_RETENTION_IN_DAYS | Log view retention in days. (default: 2) |
