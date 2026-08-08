# jvm-toolbox

`jvm-toolbox` is a Java library for small, reusable JVM abstractions with precise contracts,
behavior-first tests, and measured performance. It is intentionally not a collection of unrelated
helpers.

The first prerelease contains bounded micro-batching and keyed batch loading. The published runtime
uses only the JDK and requires Java 25 or later.

## Installation

Gradle:

```kotlin
dependencies {
    implementation("org.jcube:jvm-toolbox:0.1.0-alpha.1")
}
```

Maven:

```xml
<dependency>
  <groupId>org.jcube</groupId>
  <artifactId>jvm-toolbox</artifactId>
  <version>0.1.0-alpha.1</version>
</dependency>
```

The repository is prepared to produce this artifact, but an external Maven repository must host it
before these coordinates can be resolved outside a local publication.

## Batching tools

| Tool | Use it when | Backend receives |
| --- | --- | --- |
| `MicroBatcher<I, O>` | Every request is an independent positional operation, such as a write, bulk RPC, or inference call. | An ordered `List<I>` and returns one `BatchOutcome<O>` per position. |
| `KeyBatchLoader<K, V>` | Requests are lookups and equal keys should share work within a batching window. | A `Set<K>` of unique keys and returns a map of found or failed keys. |

Both tools provide bounded pending capacity, explicit admission policies, bounded backend
concurrency, independent cancellation, graceful draining, per-item failures, and opt-in observation.
`KeyBatchLoader` additionally distinguishes missing values with `Optional.empty()` and never acts as a
persistent cache.

## Application shape

```text
concurrent callers -> bounded admission -> batch formation
                                      -> bounded backend calls -> independent futures
```

A batching instance is normally a long-lived application component for one logical backend
operation. Create it during application startup, share it across requests, stop frontend admission
during shutdown, close the batcher so admitted work drains, and only then close its backend resources.
Do not create and close a batcher for every request.

Limits apply per instance. If several batchers or application replicas use the same backend, account
for their combined concurrency when sizing that backend.

## Minimal `MicroBatcher` use

```java
var config = new BatchingConfig(
        128,
        Duration.ofNanos(500_000),
        8,
        16_384,
        AdmissionPolicy.REJECT,
        Duration.ZERO);

BatchProcessor<Command, Result> backend = commands -> {
    List<Result> results = client.executeBatch(commands); // same order and size
    return results.stream().map(BatchOutcome::success).toList();
};

// Keep this instance as part of application state.
var batcher = new MicroBatcher<>(config, backend);

CompletableFuture<Result> result = batcher.submit(command);
CompletionStage<ApiResponse> response = result.thenApply(ApiResponse::ok);

// During application shutdown, after stopping new requests:
batcher.close();
```

Admission occurs synchronously inside `submit`; processing and completion are asynchronous after
admission. Event-loop servers should normally use `REJECT` and translate overload into an application
response. Virtual-thread handlers may use waiting admission or blocking completion when that
programming model is preferred.

See [batching](docs/batching.md) for a complete non-keyed JDBC backend, API-server integration,
configuration, failure, cancellation, shutdown, and observation guidance. See [key batch
loading](docs/key-batch-loader.md) for a PostgreSQL lookup backend and keyed semantics.

## Documentation

- [Batching and API-server integration](docs/batching.md)
- [Key batch loading and backend integration](docs/key-batch-loader.md)
- [Performance evidence and configuration methodology](docs/performance.md)
- [Development, documentation, and release workflows](docs/development.md)
- Generated API Javadocs under `build/docs/javadoc` after running `just docs`

Repository-only JMH, synthetic-load, profiling, and PostgreSQL tooling support development. They are
not part of the published artifact.

## Development

Use JDK 25 and the checked-in Gradle wrapper:

```text
just test                 # behavior tests
just check                # tests, Javadocs, source-set compilation, dependency policy
just bench-quick          # short JMH smoke run
just perf-quick           # synthetic system smoke run
just publication-check    # publication artifacts, POM, and Gradle metadata
```

Longer performance and PostgreSQL workflows are documented in [development.md](docs/development.md).

Licensed under the [MIT License](LICENSE).
