# [RFC] Specialized Metadata Record

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 009 | [@deryrahman](https://github.com/deryrahman) | Draft | 2025-06-11 | 2025-06-11 |

## Objective
This RFC proposes a standardized way to give pure metadata information to the sink component. There will be 1 or more records that contains pure metadata information. This is special record that will be consumed by the sink component but not to be processed as data. This metadata record will be used to configure the sink component dynamically.

## Terminology
- **Specialized Metadata Record**: A record that contains metadata information only, not to be processed as data.

## Requirement
To support specialized metadata records, we need to have determine what kind of data should be added as part of that records. Some requirements that need to be fulfilled:
- A record can be called specialized metadata record if it contains only metadata information.
- Specialized metadata record will not be processed as data, but will be consumed by the sink component.

## Example
Below is the record of specialized metadata record that will be consumed by the sink component:
```json
{
    "__METADATA__record_count": 100,
    "__METADATA__email_address": "sample@example.com",
}
```
And below is the record that considered as data record:
```json
{
    "column_1": "value_1",
    "column_2": "value_2",
    "__METADATA__record_count": 100,
    "__METADATA__email_address": "sample@example.com",
    "__METADATA__another_field": "data"
}
```

Record in the first example is a specialized metadata record, while the second example is a data record that contains metadata information. The sink component should be able to differentiate between these two records and process them accordingly.
