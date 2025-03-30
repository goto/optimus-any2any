# [RFC] Non-Channel Data Transfer

| RFC | Author | Status | Created Date | Updated Date |
| --- | ------ | ------ | ------------ | ------------ |
| 008 | [@deryrahman](https://github.com/deryrahman) | Declined | 2025-03-30 | 2025-03-30 |

## Objective
Currently we use channel to transfer data between source and sink. This RFC proposes a way to transfer data without using channel. Channel is considerably slower than direct io transfer. Using channel also leads to unecessary routines that hurts the performance a lot. This RFC proposes a way to transfer data without using channel.

## Rationale
Below is simple benchmark for transfering 1mil records using buffered and unbuffered channel vs direct io transfer.

| Method               | Benchmark         | Iterations | Time per Operation (ns/op) | Throughput (MB/s) | Memory Allocated (B/op) | Allocations per Operation |
|-----------------------|-------------------|------------|----------------------------|-------------------|--------------------------|---------------------------|
| Unbuffered channel    | BenchmarkYield    | 10         | 126,356,859                | 87.06             | 160                      | 2                         |
| Buffered channel (64)   | BenchmarkYield    | 10         | 34,813,478                 | 315.97            | 2,148                    | 4                         |
| Direct io transfer    | BenchmarkPipe     | 10         | 7,017,207                  | 1,567.58          | 128,279                  | 17                        |

Based on the above benchmark, it's clear that direct io transfer is much faster than using channel. The performance difference is huge, and using channel is not recommended for transferring large amount of data.

## Specification
The specification for non-channel data transfer is simple. Rather that using channel as the medium for transferring data, we will leverage io.Pipe to transfer data directly between source and sink. The source will write the data to the io.Pipe, and the sink will read the data from the io.Pipe. This will eliminate the need for channel and improve the performance significantly.

There's no need to change the existing source and sink code. We use relatively same interface for source and sink. The only change is in the underlaying Core component.

## POC Result
The POC result is very promising. The performance is significantly improved, and the code is much simpler to understand. We use file as a source and sink for the POC. With 1mil records, the performance comparison is as follows:

```sh
$ du -h test/in5.txt
39M    test/in5.txt
```

| Method               | Total Allocation | Execution Time |
|-----------------------|------------------|----------------|
| Unbuffered Channel    | 3.38 GB          | 10321 ms       |
| Buffered Channel (64) | 3.38 GB          | 8885 ms        |
| Direct IO Transfer (POC)    | 389.20 MB        | 4666 ms        |

## Implementation Result Summary
During implementation, we found that the performance is not as good as the POC. The performance is slightly better than using channel, but no significant improvement. The reason for this is that there's some extra processing involved in actual sink and source components that are not captured in the POC. Hence, we decided to not proceed with this RFC.

| Method               | Execution Time |
|-----------------------|------------------|
| Unbuffered Channel    | 10841 ms       |
| Buffered Channel (64) | 10019 ms        |
| Direct IO Transfer    | 11617 ms        |

Although the performance is not as good as expected, we still believe that this approach is worth exploring in the future. The code is much simpler to understand, and the performance is still better than using channel. We will keep this RFC open for future reference and exploration.
