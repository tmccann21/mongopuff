# mongopuff
mongopuff is a logical replication layer between MongoDB and turbopuffer. It listens to MongoDB's change stream and
persists documents in turbopuffer according to user defined mappings. Actively being built :)


# Benchmarks
The following benchmarks were measured on a single github action runner. Throughput was measured using an artificial
flush latency to simulate Turbopuffer's API delay.

| flush latency | throughput |
|---------------|------------|
| 0ms | 697384 events/sec |
| 100ms | 10007 events/sec |
| 500ms | 2032 events/sec |
| 850ms | 1197 events/sec |

According to Turbopuffer's published p50, p90, and p99 latency's the following throughput should be possible

| percentile | write latency | throughput |
|------------|---------------|------------|
| p50 | 165ms | 6113 events/sec |
| p90 | 248ms | 4082 events/sec |
| p99 | 850ms | 1197 events/sec |

Memory usage is fairly efficient for mongopuff, even when scaling to > 1000 collections. Memory usage is mainly bounded by batch size
and flush interval. Large batches with long flush intervals will see memory usage grow but this intended to be tuned according to
your use case.

| collections | peak RSS | bytes/event | per-collection throughput |
|-------------|----------|-------------|--------------------------|
| 1 | 15.7 MB | 1199 | 10010/s |
| 10 | 19.5 MB | 1197 | 9460/s |
| 50 | 34.2 MB | 1238 | 7690/s |
| 100 | 34.0 MB | 1244 | 5292/s |
| 500 | 36.3 MB | 1130 | 1016/s |
