# [RFC] Shared Metadata Between Source And Sink

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 007 | [@deryrahman](https://github.com/deryrahman) | Accepted | 2025-02-26 | 2025-03-16 |

## Objective
Some sink requires metadata from the source to process the data correctly. This metadata is procesed as part of sink dynamic configuration and not part of the data itself. This RFC proposes a way to share metadata between source and sink.

## Requirement
To support shared metadata between source and sink, we need to have a flexible way to pass metadata from source to sink. Some requirements that need to be fulfilled:
- Use record information for grouping the records (case: OSS/SFTP/SMTP sink that send the group of records in a single file)
- Use record information for filtering the records (case: filtering the records based on the record information)
- Dynamic configuration based on the record information (case: SMTP sink that needs to send the email to the destination address based on the record information)

## Specification
Instead of providing static metadata, dynamic configuration should be provided in the record itself, but not part of the data. Format of the records:

```json
{
    "__METADATA__field_1": "value_1",
    "__METADATA__field_2": "value_2",
    "column_1": "value_1",
    "column_2": "value_2"
}
```

Metadata prefix is determined by common config `METADATA_PREFIX=__METADATA__`. Metadata prefix is used to differentiate between metadata and data. Metadata prefix is configurable so that user can customize the prefix according to their needs.

## Example

### Grouping Records In A Single File Based On Certain Column

Data fetched from source:

```json
{
    "group_id": "group_1",
    "column_1": "value_1",
    "column_2": "value_2"
}
{
    "group_id": "group_1",
    "column_1": "value_3",
    "column_2": "value_4"
}
{
    "group_id": "group_2",
    "column_1": "value_5",
    "column_2": "value_6"
}
```

We want to group the records based on `group_id` and send the group of records in a single file. The sink component should be able to read the `group_id` from the metadata and group the records based on the `group_id`. With the help of `JQ__QUERY`, we can transform the data before sending it to the sink.

```bash
JQ__QUERY='del(.group_id) + {("__METADATA__group_id"): .group_id}'
```

The transformed data:

```json
{
    "__METADATA__group_id": "group_1",
    "column_1": "value_1",
    "column_2": "value_2"
}
{
    "__METADATA__group_id": "group_1",
    "column_1": "value_3",
    "column_2": "value_4"
}
{
    "__METADATA__group_id": "group_2",
    "column_1": "value_5",
    "column_2": "value_6"
}
```

Then in OSS sink, we can pass this configuration:

```bash
OSS__DESTINATION_URI="oss://bucket/path/file_[[ .__METADATA__group_id ]].json"
```

## Rationale of the Design

### Metadata in the record gives more flexibility
By providing metadata in the record itself, we can achieve dynamic configuration based on the record information. This can't be done using static metadata based on the configuration. For example sending the email to the destination address that are dynamically configured based on the record information. Sample:

```html
Hello [[ .__METADATA__name ]], </br>
This is the body of the email. You got this value: {{ .ENV_STATIC }} from static configuration.
```

```bash
JQ__QUERY='del(.name) + {("__METADATA__name"): .name} + del(.email) + {("__METADATA__email"): .email}'
SMTP__EMAIL_TO='[[ .__METADATA__email ]]'
ENV_STATIC='static value'
```

```json
{
    "name": "user1",
    "email": "user1@google.com",
    "column_1": "value_1",
    "column_2": "value_2"
}
{
    "name": "user2",
    "email": "user2@google.com",
    "column_1": "value_3",
    "column_2": "value_4"
}
{
    "name": "user3",
    "email": "user3@google.com",
    "column_1": "value_5",
    "column_2": "value_6"
}
```

Records that are sent to the sink:

```json
{
    "__METADATA__name": "user1",
    "__METADATA__email": "user1@google.com",
    "column_1": "value_1",
    "column_2": "value_2"
}
{
    "__METADATA__name": "user2",
    "__METADATA__email": "user2@google.com",
    "column_1": "value_3",
    "column_2": "value_4"
}
{
    "__METADATA__name": "user3",
    "__METADATA__email": "user3@google.com",
    "column_1": "value_5",
    "column_2": "value_6"
}
```
