# jvm-toolbox

`jvm-toolbox` is a Java 25 library for small, reusable JVM abstractions with precise contracts,
behavior-first tests, and measured performance. It is intentionally not a collection of unrelated
helpers.

## Artifacts

| Artifact | Use it for | Runtime dependencies |
| --- | --- | --- |
| `jvm-toolbox-batching` | Batching, keyed loading, and request coalescing | JDK only |
| `jvm-toolbox-bulkhead` | Bounded concurrent calls to finite downstream resources | JDK only |
| `jvm-toolbox-consumers` | Ordered, parallel Apache Kafka record processing | Apache Kafka client |

Gradle:

```kotlin
dependencies {
    implementation("io.github.jo-cube:jvm-toolbox-batching:VERSION")
    implementation("io.github.jo-cube:jvm-toolbox-bulkhead:VERSION")
    implementation("io.github.jo-cube:jvm-toolbox-consumers:VERSION")
}
```

Maven:

```xml
<dependencies>
  <dependency>
    <groupId>io.github.jo-cube</groupId>
    <artifactId>jvm-toolbox-batching</artifactId>
    <version>VERSION</version>
  </dependency>
  <dependency>
    <groupId>io.github.jo-cube</groupId>
    <artifactId>jvm-toolbox-bulkhead</artifactId>
    <version>VERSION</version>
  </dependency>
  <dependency>
    <groupId>io.github.jo-cube</groupId>
    <artifactId>jvm-toolbox-consumers</artifactId>
    <version>VERSION</version>
  </dependency>
</dependencies>
```

Add only the artifact you use. Published GitHub Releases are published to Maven Central.

## Bounded downstream calls

`Bulkhead` bounds concurrent calls to one logical downstream operation and optionally admits a
bounded number of waiting callers. It invokes blocking work or starts asynchronous work on the
admitting caller, owns no executor or lifecycle, and releases capacity across success, failure, and
asynchronous completion.

See [bulkhead](bulkhead/docs/bulkhead.md) for admission, interruption, cancellation, and execution
contracts.

## Batching and request coalescing

`MicroBatcher<I, O>` batches independent positional operations. `KeyBatchLoader<K, V>` additionally
coalesces equal lookup keys within a batching window. Both provide bounded admission and backend
concurrency with explicit failure, cancellation, shutdown, and observation contracts.

`SingleFlight<K, V>` instead coalesces equal keys for the full lifetime of an asynchronous operation.
It has no batching, caching, admission, executor, or lifecycle machinery.

See [batching](batching/docs/batching.md), [key batch loading](batching/docs/key-batch-loader.md), and
[single-flight request coalescing](batching/docs/single-flight.md).

## Ordered consumers

`LaneConsumer<K, V>` owns one Kafka consumer and fans records from its assigned partitions into a
configurable number of ordered logical lanes. Each lane forms independent micro-batches; lanes run in
parallel on virtual threads, subject to a separate concurrency limit. Fixed topic collections and
regular-expression topic subscriptions are supported. Sparse observed offsets are committed only
through the safe completion frontier.

The setup deliberately provides at-least-once delivery, fail-fast processing, bounded buffering,
cooperative cancellation on rebalance, and no retry or graceful drain framework. Downstream effects
must be idempotent because interruption cannot guarantee that old work stops before reassignment.

See [ordered consumers](consumers/docs/consumers.md) for routing, lifecycle, backpressure, offset, and
rebalance contracts.

## Development

Use JDK 25 and the checked-in Gradle wrapper:

```text
just test                 # behavior tests
just check                # tests, Javadocs, and source-set compilation
just bench-quick          # batching and bulkhead JMH smoke run
just perf-quick           # batching system smoke run
just publication-check    # publication artifacts and metadata
```

The repository is a Gradle multi-project build. Published code lives in `batching`, `bulkhead`, and
`consumers`; the root project is an unpublished build aggregator. See
[development](docs/development.md) and [performance](batching/docs/performance.md).

Licensed under the [MIT License](LICENSE).

Apache Kafka, Kafka, and Apache are trademarks of the Apache Software Foundation. This project is not
affiliated with or endorsed by the Apache Software Foundation.
