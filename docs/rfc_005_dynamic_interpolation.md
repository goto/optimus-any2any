# [RFC] Dynamic Interpolation

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 005 | [@deryrahman](https://github.com/deryrahman) | Accepted | 2025-02-26 | 2025-02-26 |

## Objective
Gives flexibility to the user to interpolate variable based on the value of the record. This is useful to achieve dynamic configuration / variable based on the record information.

## Requirement
To support dynamic interpolation, we need to have a flexible way to interpolate variable based on the record information. Some requirements that need to be fulfilled:
- Any information in the record can be used to interpolate the variable
- The interpolation should be configurable so that user can customize the interpolation according to their needs

## Approach
Utilizing golang template capability, for any information in the record can be accessed using `[[ .field_name ]]`. This rendering process will be executed in plugin level. The interpolation will use golang template with `[[` and `]]` delimiters to avoid conflict with the existing interpolation.

## Limitation
- The interpolation is only available in the sink component
- Some configuration that expected to be static might not be supported to use dynamic interpolation. e.g. `CREDENTIALS`, `BATCH_SIZE`, etc
