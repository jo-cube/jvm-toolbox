# jvm-toolbox maintainer guide

`jvm-toolbox` provides small, reusable JVM abstractions with precise contracts, behavior-first tests,
useful documentation, and measured performance. It is not a home for miscellaneous shared code.

## Baseline and boundaries

- Production is Java 25 without preview APIs.
- The published coordinate is `org.jcube:jvm-toolbox`; packages start with `org.jcube.jvmtoolbox`.
- The production runtime has no dependencies. Keep JUnit, JMH, HdrHistogram, JDBC, Docker, and profiling
  dependencies outside `src/main`.
- Public APIs use Java-native types and must not expose queues, executors, schedulers, virtual-thread
  machinery, database adapters, or monitoring frameworks.
- New toolbox functionality must be a coherent reusable abstraction with standalone value, not a
  `utils`, `common`, `helpers`, or `misc` collection.

## Repository map

```text
src/main/java/       published API and runtime
src/test/java/       contract and concurrency tests
src/jmh/java/        JMH mechanical benchmarks
src/perf/java/       system profilers, synthetic loads, and PostgreSQL tooling
src/perf/postgres/   database schema and seed scripts
docs/                maintained user and contributor documentation
```

If a `.codegraph` index exists, use `codegraph explore` before text search when locating or
understanding code and call paths.

## Documentation routing

Read the smallest maintained guide that covers the task:

- `README.md`: product positioning, installation, and abstraction choice.
- `docs/batching.md`: `MicroBatcher`, non-keyed backends, server integration, admission, failure,
  cancellation, shutdown, and observability.
- `docs/key-batch-loader.md`: keyed backends, coalescing, missing values, fan-out, and cache scope.
- `docs/performance.md`: measurement evidence, configuration methodology, and performance workflows.
- `docs/development.md`: repository, documentation, CI, and publication workflows.

Public Java types and Javadocs define the API surface, and behavior tests freeze observable contracts.
Markdown guides explain how to apply them. If documentation and source disagree, investigate the
discrepancy rather than silently choosing one; update all affected contract surfaces together.

## Commands

```text
just test                 unit and concurrency tests
just check                normal full verification, including Javadocs and source-set compilation
just docs                 warning-free public Javadocs
just publication-check    publication artifacts, POM, and Gradle metadata
just bench-quick          quick JMH smoke run
just perf-quick           quick synthetic system validation
```

Longer JMH, JFR, synthetic, backend-profiler, batching-profiler, and PostgreSQL commands are documented
in `docs/performance.md`. Docker is never required for ordinary tests.

## Engineering rules

- Keep the public API smaller than the implementation. Document nullability, units, ownership,
  blocking, exceptions, thread safety, cancellation, and lifecycle behavior.
- Treat admission, oldest-request timing, result correlation, batch failure, cancellation,
  `maxConcurrentBatches`, and close/drain races as contracts.
- Add tests for observable behavior and concurrency invariants, not coverage percentages, getters, or
  private structure. Prefer deterministic clocks and explicit synchronization over sleeps.
- Comments should explain public contracts, non-obvious invariants, or memory/concurrency reasoning;
  do not narrate straightforward code.
- Keep each explanation in its canonical guide and cross-link it elsewhere. Examples should show
  long-lived application ownership, distinguish synchronous admission from asynchronous completion,
  and label framework-specific placeholders.
- Performance changes require comparable measurements. Put JMH in `src/jmh`, system and integration
  tooling in `src/perf`, and keep machine-specific thresholds out of normal CI.
- Do not introduce a dependency, extension point, module, adapter, or tuning option until a concrete
  use case justifies it.

Before handing off a change, run `just check` and the smallest relevant benchmark or integration smoke
workflow. Keep generated reports under `build/`.
