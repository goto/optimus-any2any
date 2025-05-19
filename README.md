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

**Use direct execution without data transfer (experimentational):**

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
| FILE | FILE__SOURCE_URI | Path to the input file. Format file:///directory/to/folder/or/file.ext |
| SF | SF__HOST | Salesforce instance host. |
| | SF__USER | Salesforce username. |
| | SF__PASS | Salesforce password. |
| | SF__TOKEN | Salesforce security token. |
| | SF__SOQL_FILE_PATH | Path to the SOQL query file. |
| | SF__API_VERSION | Salesforce API version. (default: 54.0) |
| | SF__INCLUDE_DELETED | Include deleted records. (default: false) |
| GMAIL | GMAIL__TOKEN | Token JSON for gmail credentials |
| | GMAIL__FILTER | Gmail filter based on gmail filter rules |
| | GMAIL__FILENAME_COLUMN | Column name for the filename. (default: __METADATA__filename) |
| | GMAIL__CSV_DELIMITER | Delimiter for CSV file format. (default: ,) |
| MC | MC__CREDENTIALS | Credentials for MaxCompute. |
| | MC__PRE_QUERY_FILE_PATH | Path to the pre sql query file. (empty for ignore) |
| | MC__QUERY_FILE_PATH | Path to the query file. |
| | MC__FILENAME_COLUMN | Column name for the filename. (default: __METADATA__filename) |
| | MC__EXECUTION_PROJECT | Project ID for the query execution. |
| | MC__ADDITIONAL_HINTS | Additional hints for the execution query. |
| | MC__LOG_VIEW_RETENTION_IN_DAYS | Log view retention in days. (default: 2) |
| | MC__BATCH_SIZE | Number of records fetched in one request. (default: 1000) |
| OSS | OSS__CREDENTIALS | Credentials for OSS. |
| | OSS__SOURCE_URI | The source path in a OSS bucket to read the files. Format oss://bucket/path/to/folder/or/file.json. |
| | OSS__CSV_DELIMITER | Delimiter for CSV file format. (default: ,) |
| | OSS__SKIP_HEADER | Skip header for CSV file format. (default: false) |
| | OSS__SKIP_ROWS | Skip rows for CSV file format. (default: 0) |

## Supported Sinks

| Component | Configuration | Description |
|---|---|---|
| FILE | FILE__DESTINATION_URI | Path to the output file. Format `file:///directory/to/somthing.extension` |
| MC | MC__CREDENTIALS | Credentials for MaxCompute. |
| | MC__DESTINATION_TABLE_ID | Destination table ID in Maxcompute. |
| | MC__LOAD_METHOD | Load method availability: APPEND, REPLACE. (default: APPEND) |
| | MC__UPLOAD_MODE | Upload mode availability: STREAM, REGULAR. (default: REGULAR) |
| | MC__CONCURRENCY | Concurrency number for REGULAR upload mode. (default: 4) |
| | MC__BATCH_SIZE_IN_MB | Batch size for the file upload. (default: 64) |
| | MC__ALLOW_SCHEMA_MISMATCH | Allow schema mismatch. (default: false) |
| | MC__EXECUTION_PROJECT | Project ID for the query execution. |
| IO | - | - |
| OSS | OSS__CREDENTIALS | Credentials for OSS. |
| | OSS__BATCH_SIZE | Batch size for the file upload. Keep empty for ignore |
| | OSS__DESTINATION_URI | The destination path in a OSS bucket to put the result files. Format `oss://bucket/path/to/file.extension` |
| | OSS__ENABLE_OVERWRITE | Flag to overwrite the file based on destination bucket path. |
| | OSS__SKIP_HEADER | Skip header for CSV file format. (default: false) |
| | OSS__MAX_TEMP_FILE_RECORD_NUMBER | Maximum number of record that temp files can store before it is uploaded to OSS. (default: 50000) |
| | OSS__COMPRESSION_TYPE | If set, the resulting files will be archived into the selected type. Supported types: `gz`, `tar.gz`, and `zip` |
| | OSS__COMPRESSION_PASSWORD | Set password for the resulting archive file. Only supported for `zip` comporession_type |
| S3 | S3__CREDENTIALS | Credentials for S3. |
| | S3__PROVIDER | S3 provider availability: AWS, TIKTOK. (default: AWS) |
| | S3__REGION | S3 region for the bucket. (default: us-east-1) |
| | S3__BATCH_SIZE | Batch size for the file upload. Keep empty for ignore |
| | S3__DESTINATION_URI | The destination path in a S3 bucket to put the result files. Format `s3://bucket/path/to/file.extension` |
| | S3__ENABLE_OVERWRITE | Flag to overwrite the file based on destination bucket path. |
| | S3__SKIP_HEADER | Skip header for CSV file format. (default: false) |
| | S3__MAX_TEMP_FILE_RECORD_NUMBER | Maximum number of record that temp files can store before it is uploaded to S3. (default: 50000) |
| SFTP | SFTP__PRIVATE_KEY | SFTP private key for authentication. "" for ignore |
| | SFTP__HOST_FINGERPRINT | SFTP host fingerprint for authentication. "" for ignore |
| | SFTP__DESTINATION_URI | Following the [rfc2396 format](https://datatracker.ietf.org/doc/html/rfc2396) `sftp://user[:password]@host[:port]/path/to/folder/or/file.extension` |
| | SFTP__COMPRESSION_TYPE | If set, the resulting files will be archived into the selected type. Supported types: `gz`, `tar.gz`, and `zip` |
| | SFTP__COMPRESSION_PASSWORD | Set password for the resulting archive file. Only supported for `zip` comporession_type |
| SMTP | SMTP__CONNECTION_DSN | SMTP connection dsn, format `smtp://user[:password]@host[:port]`, if port is not specified, default port `587` will be used. user and password should be url encoded. |
| | SMTP__FROM | SMTP from email address. |
| | SMTP__TO | SMTP to, cc, and bcc email address, format: `to:email@address.tld[,another@address.tld]...[;cc:(,another@address.tld)...][;bcc:(,another@address.tld)...]` |
| | SMTP__SUBJECT | SMTP email subject. |
| | SMTP__BODY_FILE_PATH | SMTP email body from given path. |
| | SMTP__ATTACHMENT_FILENAME | SMTP email attachment filename. |
| | SMTP__STORAGE_MODE | SMTP email storage mode. Support attachment (default) and OSS. If set to OSS, attachments will be sent as OSS signed URL |
| | SMTP__STORAGE_DESTINATION_DIR | If storage mode is set to other than attachment, this will be the root directory where the generated attachment will be uploaded |
| | SMTP__STORAGE_LINK_EXPIRATION | If storage mode is set to other than attachment, the generated signed URLs expiration will be set to this number (seconds) |
| | SMTP__STORAGE_CREDENTIALS | If storage mode is set to other than attachment, this credential will be used to authenticate to the bucket |
| | SMTP__COMPRESSION_TYPE | If set, the resulting files will be archived into the selected type. Supported types: `gz`, `tar.gz`, and `zip` |
| | SMTP__COMPRESSION_PASSWORD | Set password for the resulting archive file. Only supported for `zip` comporession_type |
| POSTGRES | PG__CONNECTION_DSN | Postgres connection DSN. |
| | PG__DESTINATION_TABLE_ID | Destination table ID in Postgres. |
| | PG__PRE_SQL_SCRIPT | SQL script to run before the data transfer. |
| | PG__BATCH_SIZE | Batch size for the record inserted in one request. (default: 512) |
| REDIS | REDIS__CONNECTION_DSN | DSN redis for connection establishment `redis[s]://[[username][:password]@]host[:port][,host2[:port2]...][/db-number]` |
| | REDIS__CONNECTION_TLS_CERT | Redis TLS certificate. (optional) |
| | REDIS__CONNECTION_TLS_CACERT | Redis TLS CA certificate. (optional) |
| | REDIS__CONNECTION_TLS_KEY | Redis TLS key. (optional) |
| | REDIS__RECORD_KEY | Key for the record inserted, eg `example:key:[[ .field_key ]]` |
| | REDIS__RECORD_VALUE | Value for the record inserted, eg `[[ .field_value ]]` or `[[ . \| tojson ]]` |
| | REDIS__BATCH_SIZE | Batch size for the record inserted in one request. (default: 512) |
| HTTP | HTTP__METHOD | HTTP method availability: POST, PUT, PATCH |
| | HTTP__ENDPOINT | HTTP endpoint to send the data. |
| | HTTP__HEADER | HTTP headers for the request, key:value with comma separated |
| | HTTP__HEADERS_FILE_PATH | HTTP headers loaded from file. |
| | HTTP__BODY | HTTP body for the request. |
| | HTTP__BODY_FILE_PATH | HTTP body loaded from file. |
| | HTTP__BATCH_SIZE | Batch size for the HTTP request. |
| KAFKA | KAFKA__BOOTSTRAP_SERVERS | Kafka bootstrap servers, comma-separated. |
| | KAFKA__TOPIC | Kafka topic to write the data. |

## Supported Processors

| Component | Configuration | Description |
|---|---|---|
| JQ | JQ__QUERY | Any valid jq query. If set, it will override the query from file path. |
| | JQ__QUERY_FILE_PATH | Any valid jq query loaded from file. |
| | JQ__BATCH_SIZE | Batch size for the JQ processor. (default: 512) |
| | JQ__BATCH_INDEX_COLUMN | Column name for the batch index. (default: __METADATA__jq_batch_index) |

## Common Configuration

| Configuration | Description |
|---|---|
| LOG_LEVEL | Log level for the application. (default: INFO) |
| BACKEND | Backend to be used, CHANNEL / IO (default: IO) |
| BUFFER_SIZE | Buffer size for the data transfer. (default: 0 / no buffer) |
| OTEL_COLLECTOR_GRPC_ENDPOINT | OpenTelemetry collector gRPC endpoint. |
| OTEL_ATTRIBUTES | OpenTelemetry attributes for tracing. key=value with comma separated. |
| METADATA_PREFIX | Metadata prefix for the data transfer. (default: __METADATA__) |
| RETRY_MAX | Maximum number of retries for the data transfer. (default: 3) |
| RETRY_BACKOFF_MS | Backoff time in milliseconds for the retry. (default: 1000) |
| DRY_RUN | Enable dry run mode. (default: false) |
| ENABLE_PPROF | Enable pprof for profiling. (default: false) |

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
