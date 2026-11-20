# Broker monitoring

The Broker exports metrics through `/actuator/prometheus`. The management HTTP
server has a separate port, but shares the Broker JVM and its CPU/memory budget.
Metric collection must remain cheap when commands, replication, or storage are
under load.

## Metric inventory and changes

|       Area       |                                                         Metrics retained                                                          |                                                      Reduction                                                       |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| Engine           | Job events by action/type, incident events, pending incidents                                                                     | Event counters remain lazy; no job-key, request-ID, or worker-ID labels                                              |
| Commands         | Queue depth at submission, command Raft append and processing latency                                                             | Replace broad default histogram buckets with explicit boundaries                                                     |
| Log append       | Records and bytes per atomic append                                                                                               | Replace 276 default buckets per distribution with explicit boundaries                                                |
| Stream processor | Record rates/high-water marks, batch size/duration, continuations, retries, result shapes, post-commit and internal phase latency | Record batch execution time once; remove the duplicate `engine_batch` phase                                          |
| Snapshots        | Triggers by reason, attempts by result, retries, successful/failed attempt latency, log bytes, snapshot size/index                | Keep reason on the trigger counter; omit it from latency. Skipped attempts retain a counter but no latency histogram |
| JVM/process      | CPU, memory, GC, threads, file descriptors, disk capacity                                                                         | Retain standard resource metrics for diagnosing saturation                                                           |
| Grafana          | Job/incident, processing, append, snapshot, JVM and PD health panels                                                              | Remove unsupported process/flow-node panels; use Job completion in the overview                                      |

`taskite_stream_processor_batch_duration_seconds_*` is the sole batch execution
latency metric. The `engine_batch` label value is removed from
`taskite_stream_processor_phase_duration_seconds_*`; queue wait, result Raft
commit, state commit, and response delivery remain separate phases.

`taskite_snapshot_duration_seconds_*` retains the `result` label for `success`
and `failure`, without `reason` or `skipped`. Snapshot trigger and attempt
counters still expose why an attempt started and whether it was skipped.
The snapshot collector is shared with PD, whose existing dashboard aggregates
latency by result and therefore uses the same reduced series.

## Histogram policy

Histograms use explicit service-level boundaries instead of
`publishPercentileHistogram()`. Prometheus still exports cumulative `_bucket`
samples and an automatic `+Inf` bucket, so existing `histogram_quantile()`
queries continue to work. `_count`, `_sum`, and `_max` remain available.

|               Distribution                |                                         Finite boundaries                                         |
|-------------------------------------------|---------------------------------------------------------------------------------------------------|
| Command depth, commands/records per batch | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 2000, 3000, 4096, 8192, 16384, 32768, 65536          |
| Append bytes                              | 256B through 16MiB, with powers of two plus an explicit 3MiB boundary                             |
| Command/processing latency                | 10µs through 60s, with finer boundaries around sub-millisecond work and 100ms–1s response latency |
| Snapshot latency                          | 10ms through 30min                                                                                |

Boundaries limit histogram series, not observations: values above the largest
finite boundary still contribute to `+Inf`, count, sum, and max. P95/P99 are
interpolated estimates within the chosen buckets, rather than exact values.
If a percentile reaches the final finite boundary, inspect `+Inf` and `_max`
before treating that boundary as the actual tail latency.

## Export comparison

Measured with Micrometer 1.13.4 and the same five collector types for 15
partitions: command dispatch, log append, stream processing, engine, and
snapshots. All snapshot trigger reasons and results were exercised; lazy
job/incident action counters and standard JVM binders were excluded from this
comparison. Both versions used identical cluster/broker/partition labels.

|            Export            |    Before |   After | Reduction |
|------------------------------|----------:|--------:|----------:|
| Registered meters            |       780 |     615 |     21.2% |
| Samples per scrape           |    39,825 |   5,625 |     85.9% |
| Histogram bucket samples     |    38,295 |   4,590 |     88.0% |
| UTF-8 response bytes         | 5,708,963 | 772,838 |     86.5% |
| Local scrape encoding median |    28.9ms |   4.6ms |     84.0% |

Encoding times are a local warm-up comparison (30 warm-up scrapes, 100 measured
scrapes), not a network or production load test. Real exports also include JVM
metrics and active job types, so their size varies. The Actuator endpoint
regression checks that 15 partitions plus standard resource metrics stay below
900KB and 6,000 business samples, that overflow reaches `+Inf`/count/sum, and
that collectors can be removed and recreated without retaining old samples.

## Metric semantics

Queue depth is measured when a command is submitted; it is not a gauge of the
current number of in-flight commands. Processed/committed record gauges are log
high-water positions, and pending records are the nonnegative difference between
the committed and consumed positions.

## Scrape configuration

For the currently observed remote Broker endpoints, a starting configuration is:

```yaml
- job_name: taskite-broker
  scrape_interval: 30s
  scrape_timeout: 25s
  metrics_path: /actuator/prometheus
  # Keep the actual targets in static_configs or service discovery.
```

`scrape_interval` does not increase `scrape_timeout`; the latter defaults to 10s
and must not exceed the interval. Set the Grafana Prometheus data source scrape
interval (or query minimum step) to match the actual interval so
`$__rate_interval` contains enough samples.

Check `up{job="taskite-broker"}` and `scrape_duration_seconds` before interpreting
missing series as zero business activity. Job/incident action counters may be
absent until an event occurs; a failed scrape must remain distinguishable from
zero events.

The repository dashboard is `grafana/broker.json`. Broker code changes require
a rebuilt/redeployed distribution; dashboard changes require importing the
updated JSON into Grafana.
