# [RFC] Filesystem Sink

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 006 | [@deryrahman](https://github.com/deryrahman) | Accepted | 2025-02-27 | 2025-02-27 |

## Objective
To provide a general design of sink component that writes the record to the various filesystem.

## Requirement
Records are send in the form of json format in individual manner. The records should be successfully written to the filesystem by this following requirements:
- Records can be grouped by the user defined key
- Records without grouping key will be written as 1 file
- The file can be splited into multiple files based on batch size
- File path naming should be configurable

## Specification
As the records are received individually, the sink component will buffer the records based on the grouping key. Once the bucket meets its buffer size, the sink component will flush the records to the filesystem. This requires filesystem handler to support various destination paths.

### Filesystem Handler
The filesystem handler is responsible to:
- Flush the records to the filesystem
- Close filesystem io when it's done

One sink component can have multiple filesystem handler. Each handler will responsible to write the records to the different filesystem based on the grouping key.
