# [RFC] Connector For Data Transformation

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 002 | [@deryrahman](https://github.com/deryrahman) | Accepted | 2024-12-09 | 2025-02-26 |

## Objective
There's a need to transform data between data sources and data sinks. This RFC proposes a new connector component that can transform data between different data sources and data sinks.

## Approach
The new connector component will be responsible for transforming data between data sources and data sinks. It will have the following responsibilities:
- mutating data
- filtering data
- generating new data
- aggregating data

This connector is aimed to be a flexible component that can be easily customized to handle different data transformation requirements. As we use JSON as the intermidiate data format, the connector will be designed to work with JSON data. Hence, [JQ](https://jqlang.org/) will be used as the query language for data transformation.

### Specification
For connector, there's no prefix needed for the name on env var. For the initial implementation, `JQ__QUERY` will be used as the env var to define the JQ query for data transformation. Moreover `JQ__QUERY_FILE_PATH` is used to define complex JQ query that couldn't fit in the env var.

### Implementation Candidates
**jq binary**

The jq binary will be used to transform the data. The jq binary will be executed as a separate process and will be used to transform the data. Records need to be passed between channel.

**existing libary** (choosen)

Existing golang library that can be used directly in golang code. Eg. [gojq](https://github.com/itchyny/gojq). The jq query will be inferred for every records.