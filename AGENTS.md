# jvm-toolbox maintainer guide

`jvm-toolbox` provides small, reusable JVM abstractions with precise contracts, behavior-first tests,
useful documentation, and measured performance. It is not a home for miscellaneous shared code.

## Baseline and boundaries

- Production is Java 25 without preview APIs; packages start with `org.jcube.jvmtoolbox`.
- Published artifacts are `io.github.jo-cube:jvm-toolbox-batching`,
  `io.github.jo-cube:jvm-toolbox-bulkhead`, and `io.github.jo-cube:jvm-toolbox-consumers`. The root
  project is not published.
- `batching` and `bulkhead` have no production dependencies. `consumers` intentionally exposes
  Apache Kafka record and deserializer types and has only `kafka-clients` as a production dependency.
- Public APIs must not expose internal queues, executors, schedulers, virtual-thread machinery,
  database adapters, or monitoring frameworks.
- New functionality must be a coherent reusable abstraction with standalone value, not a `utils`,
  `common`, `helpers`, or `misc` collection.

## Repository map

```text
batching/src/main/java/       batching published API and runtime
batching/src/test/java/       batching contract and concurrency tests
batching/src/jmh/java/        JMH mechanical benchmarks
batching/src/perf/            system profilers, loads, and PostgreSQL tooling
batching/docs/                batching and performance guides
bulkhead/src/main/java/       bulkhead published API and runtime
bulkhead/src/test/java/       bulkhead contract and concurrency tests
bulkhead/src/jmh/java/        bulkhead mechanical benchmarks
bulkhead/docs/                bulkhead guide
consumers/src/main/java/      consumers published API and runtime
consumers/src/test/java/      consumer contract and concurrency tests
consumers/src/integrationTest/ broker-backed consumer integration tests
consumers/docs/               ordered-consumer guide
docs/development.md           repository, CI, and publication workflows
```

If a `.codegraph` index exists, use `codegraph explore` before text search when locating or
understanding code and call paths.

## Documentation routing

Read the smallest maintained guide that covers the task:

- `README.md`: product positioning, installation, and artifact choice.
- `batching/docs/batching.md`: `MicroBatcher`, non-keyed backends, admission, failure, lifecycle, and
  observability.
- `batching/docs/windowed-accumulator.md`: incremental accumulation, window boundaries, state
  ownership, backpressure, failure, and lifecycle.
- `batching/docs/key-batch-loader.md`: keyed backends, coalescing, missing values, fan-out, and cache
  scope.
- `batching/docs/single-flight.md`: full-flight request coalescing, cancellation, failure, and cache
  scope.
- `batching/docs/performance.md`: measurement evidence, methodology, and performance workflows.
- `bulkhead/docs/bulkhead.md`: capacity, admission, execution, interruption, cancellation, and scope.
- `consumers/docs/consumers.md`: routing, lane ordering, batching, offsets, backpressure, rebalance,
  failure, and shutdown.
- `docs/development.md`: repository, documentation, CI, and publication workflows.

Public Java types and Javadocs define the API surface, and behavior tests freeze observable contracts.
If documentation and source disagree, investigate and update all affected contract surfaces together.

## Commands

```text
just test                 unit and concurrency tests
just check                full verification, including warning-free Java and Javadocs
just integration-test     broker-backed consumer integration tests; requires Kafka
just docs                 warning-free public Javadocs
just publication-check    publication artifacts, POMs, and Gradle metadata
just bench-quick          quick batching and bulkhead JMH smoke run
just perf-quick           quick batching synthetic system validation
```

Longer performance commands are documented in `batching/docs/performance.md`. Docker is never
required for ordinary tests.

## Engineering rules

- Keep the public API smaller than the implementation. Document nullability, units, ownership,
  blocking, exceptions, thread safety, cancellation, and lifecycle behavior.
- For batching, treat admission, oldest-request timing, result correlation, batch failure,
  cancellation, `maxConcurrentBatches`, and close/drain races as contracts.
- For windowed accumulation, treat count/time/flush boundaries, state transfer, pending-input
  capacity, processor failure, and close/drain races as contracts.
- For consumers, treat deterministic routing, lane order, sparse offset frontiers, bounded polling,
  pause/resume, rebalance fencing, mixed-batch invalidation, commits, and no-drain shutdown as
  contracts.
- For bulkheads, treat active and waiting bounds, rejection, timeout, interruption, failure,
  cancellation, and asynchronous operation lifetime as contracts.
- Add tests for observable behavior and concurrency invariants, not coverage percentages, getters, or
  private structure. Prefer deterministic clocks and explicit synchronization over sleeps.
- Comments should explain public contracts, non-obvious invariants, or memory/concurrency reasoning;
  do not narrate straightforward code.
- Keep explanations in their canonical guide and cross-link them elsewhere.
- Performance changes require comparable measurements. Keep JMH and integration tooling under the
  owning module, and machine-specific thresholds out of normal CI.
- Do not introduce a dependency, extension point, module, adapter, or tuning option until a concrete
  use case justifies it.

Before handing off a change, run `just check` and the smallest relevant benchmark or integration
smoke workflow. Keep generated reports within the owning module's `build/` directory.
