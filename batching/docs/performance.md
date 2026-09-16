# Performance

Performance is an engineering input for `jvm-toolbox`, not a portable guarantee. Results depend on the
JDK, processor, memory, operating system, backend, workload, and measurement configuration. The
repository keeps reproducible benchmarks and profilers; generated results live under
`batching/build/reports`
and are not committed as product claims.

## Measurement layers

The repository separates three questions:

1. JMH benchmarks isolate submission, completion, batching, windowed accumulation, contention,
   admission, allocation, keyed loading, single-flight coalescing, `Optional`, and statistics costs
   with synthetic processors.
2. Synthetic system workloads exercise closed-loop and independently paced open-loop traffic,
   backpressure, backend slowdown, high foreground concurrency, latency distributions, CPU, GC, and
   memory.
3. PostgreSQL workflows first measure direct JDBC bulk capacity and then compare the same backend with
   individual requests through `MicroBatcher` or `KeyBatchLoader`.

JMH throughput and average-time results are mechanical measurements, not p95/p99 end-to-end latency.
System harnesses use high-dynamic-range histograms for latency distributions. Open-loop reports retain
offered, admitted, completed, rejected, and timed-out counts so overload cannot be hidden by completed
throughput. Open-loop latency starts at each request's scheduled offer time, so launcher and scheduling
lag remain visible.

`BackendProfiler` and `BatchingProfiler` retain raw repetitions, aggregate histograms, stability data,
resource diagnostics, and tolerance-aware Pareto analysis. They remain experimental repository tools
under `batching/src/perf`, not published APIs.

## Current conclusions

These conclusions came from the checked-in harnesses on one development machine running JDK 25. They
should guide investigation, not capacity planning:

- Batching amortizes coordination strongly. In the full JMH run, a one-request batch took about 7.1
  microseconds while a 128-request batch took about 14.9 microseconds total—roughly 0.12 microseconds
  per logical request at that batch size.
- Closed-loop synthetic runs supported up to 50,000 virtual-thread callers in the tested environment.
  The 50K run retained bounded pending work, used about 20 platform threads at peak, and showed no JFR
  evidence of carrier-thread pinning by the batching runtime.
- Asynchronous `CompletableFuture` consumption is the preferred maximum-throughput path. In the
  PostgreSQL attribution run at batch size 128 and database concurrency 8, asynchronous micro and keyed
  paths reached about 83% of direct pre-formed JDBC throughput, while the blocking virtual-thread keyed
  path reached about 55%.
- At batch size 512 and database concurrency 8, asynchronous micro batching reached about 92% and
  asynchronous keyed loading about 88% of direct JDBC throughput; blocking keyed loading reached about
  50%.
- Blocking virtual-thread consumption is supported and remained operational at high concurrency, but
  scheduling and resumption were a significant throughput, CPU, and allocation cost in that measured
  workload. This was not attributed to carrier pinning.
- Sustained load generally formed nearly full batches. Low offered load formed smaller, time-triggered
  batches as intended. Above sustainable backend capacity, `REJECT` and timed admission exposed loss,
  while `WAIT` kept library pending depth bounded and moved backlog to waiting callers.
- Synthetic and PostgreSQL JFR recordings did not identify ingress or coordinator work as a material
  bottleneck. Backend service, per-request future/completion work, and foreground scheduling dominated
  the realistic workloads. There is no current evidence for replacing the JDK coordination machinery
  with JCTools, Agrona, Disruptor, or a custom queue.

These numbers are deliberately approximate and machine-local. They are not release promises and are
not enforced in CI.

## Applying the evidence

Configure batching from backend and workload measurements rather than from the library's synthetic
maximum throughput:

1. Measure already-formed backend batches across a small batch-size and concurrency matrix. Find the
   region where throughput, latency, and backend resource use remain acceptable.
2. Set `maxBatchSize` near a useful backend operating point, not automatically at the largest batch
   the backend accepts.
3. Set `maxConcurrentBatches` within the application's share of the backend concurrency budget. A
   rough deployment upper bound is `application instances × batching instances ×
   maxConcurrentBatches`; a connection pool or backend quota may impose a lower bound in practice.
4. Choose `maxWait` from the foreground latency budget and observe batch fill under low, moderate, and
   sustained load. High load may fill batches before the timer matters; low load exposes the full
   linger tradeoff.
5. Treat `maxPendingRequests` as a memory and backlog limit. Exercise overload deliberately and verify
   that rejection or admission timeouts occur before downstream deadlines and retry amplification.
6. Validate the complete consumption model. Prefer asynchronous future composition for maximum
   throughput; measure blocking virtual-thread handlers if their simpler programming model is desired.

For a backend whose work is approximately constant per formed batch, the simple ceiling

```text
maxConcurrentBatches × batchSize / backendBatchLatency
```

is a useful comparison, not a throughput guarantee. Real systems also pay connection-pool, protocol,
serialization, result-correlation, completion, network, and frontend scheduling costs.

Use `BatchStatistics` or a custom observer to verify admission, rejection, batch size, in-flight work,
failures, and keyed coalescing in the real application. The built-in statistics deliberately omit
latency histograms; application monitoring or repository profilers should record latency where needed.
Pending samples in the repository reports are observer-side incomplete counts, so they may briefly
exceed configured admission capacity while synchronous future actions finish; admission outcomes and
the focused capacity tests verify the actual bound.

JMH identifies mechanical costs and regression candidates but does not establish end-to-end latency
or sustainable offered load. Use the synthetic and PostgreSQL harnesses for those questions, and
profile before attributing a gap to batching coordination.

## Running measurements

```text
just bench-quick                 short JMH smoke run
just bench                       full JMH suite
just bench-allocation            JMH allocation profiler
just bench-jfr                   representative JMH JFR recording
just perf-quick                  quick synthetic system workload
just perf                        full high-concurrency synthetic workload
just perf-jfr                    representative synthetic JFR recording
just perf-backend-profiler       synthetic backend sweep
just perf-batching-profiler      synthetic batching-profiler matrix
```

PostgreSQL requires Docker:

```text
just postgres-up
just postgres-seed              # defaults to 100,000 rows
just perf-postgres
just perf-postgres-attribution
just perf-postgres-batching-profiler
just postgres-down
```

Larger datasets and longer profiles are manual workflows. Performance changes require before/after
measurements using comparable environments; no machine-specific throughput threshold belongs in the
normal correctness workflow.

## Focused batching comparisons

`MicroBatcherBenchmark` measures complete submission-to-result cycles. Its `sequentialOutcomes`
parameter selects either the usual array-backed result list or a `LinkedList`, both valid processor
results. Keep the whole-batch validation pass: a malformed result must fail every position before any
success is delivered. Completion traverses the validated outcomes with an iterator so sequential
lists do not turn result correlation into quadratic work.

Measure both statistics settings and include one-request batches and the usual array-backed results
when assessing traversal changes. Preserve the existing admission coordination unless representative
system measurements support a change: fewer lock acquisitions alone do not establish a benefit.

For a focused comparison, build the JMH jar on each revision and run identical settings, retaining
separate result files under `batching/build/reports/jmh`:

```sh
./gradlew :batching:jmhJar
mkdir -p batching/build/reports/jmh
java -jar batching/build/libs/jvm-toolbox-batching-0.0.0-SNAPSHOT-jmh.jar \
  '.*MicroBatcherBenchmark.batch' \
  -p batchSize=1,128,512,4096 -p statisticsEnabled=false,true \
  -p sequentialOutcomes=false,true -bm avgt -tu us \
  -wi 3 -i 5 -w 1s -r 1s -f 2 -rf json \
  -rff batching/build/reports/jmh/comparison.json
```

Large sequential-result batches diagnose an input-size scaling problem; they are not a reason to
increase the application's batch size. Pair these mechanical measurements with `just perf-quick` or
a representative backend workload before attributing an end-to-end throughput gain to coordination.
