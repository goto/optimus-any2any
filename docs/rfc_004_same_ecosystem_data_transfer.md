# [RFC] Same Ecosystem Data Transfer

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 004 | [@deryrahman](https://github.com/deryrahman) | Accepted | 2025-01-16 | 2025-02-26 |

## Objective
Some component combinations can be on the same ecosystem, for example, OSS source component and MC sink component that are both on the same ecosystem (Alibaba Cloud). To avoid unnecessary network traffic (from and to the cloud), we need to provide a way to transfer data between components that are on the same ecosystem.

## Specification
The data transfer between components that are on the same ecosystem should be done using the internal cloud provider. The data transfer should be done using the cloud provider's internal network to avoid unnecessary network traffic. If there's an existing solution provided by the cloud provider, we should use it.

As it's using cloud specific solution, there's no guarantee to have common implementation between source and sink. Hence the combination of source and sink is predefined with dedicated flag, eg `--no-pipeline`.

## Example

OSS source component and MC sink component that are both on the same ecosystem (Alibaba Cloud) can be combined using the `--no-pipeline` flag.

```bash
$ ./bin/any2any --source=oss --sink=mc --no-pipeline
```

If user doesn't specify `--no-pipeline` flag, the data transfer will be done using the default pipeline aka pull the data out of the cloud and push the data back to the cloud.
