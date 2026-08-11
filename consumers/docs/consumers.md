# Ordered consumers

`jvm-toolbox-consumers` turns records from one owned Apache Kafka consumer into independently ordered
processing lanes:

```text
assigned partitions -> one poll owner -> deterministic routing -> ordered lanes
                                                        -> bounded virtual-thread batches
                                                        -> sparse offset frontier -> commit
```

It is deliberately an at-least-once processing primitive, not a stream-processing framework. It has
no retries, dead-letter handling, transactions, producer integration, persistent completion state, or
partial-batch success protocol.

## Installation

```kotlin
dependencies {
    implementation("io.github.jo-cube:jvm-toolbox-consumers:VERSION")
}
```

The artifact exposes Apache Kafka client record and deserializer types and therefore carries
`kafka-clients` as an API dependency.

## Basic use

```java
var properties = Map.<String, Object>of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
        ConsumerConfig.GROUP_ID_CONFIG, "invoice-workers");

var config = new ConsumerProcessingConfig(
        128,                    // ordered logical lanes
        64,                     // records per batch
        Duration.ofMillis(10),  // oldest-record wait
        64,                     // concurrent batch calls
        16_384,                 // global in-flight records
        2_048,                  // in-flight records per partition
        500,                    // Kafka max.poll.records
        Duration.ofMillis(250));// longest poll wait

try (var consumer = new LaneConsumer<>(
        properties,
        new StringDeserializer(),
        new InvoiceDeserializer(),
        List.of("invoices"),
        config,
        LaneRouter.byKeyHashCode(),
        records -> invoiceClient.store(records))) {
    consumer.run(); // blocks until another thread closes it or processing fails
}
```

The library owns the supplied deserializers and constructs, subscribes, polls, commits, and closes the
Kafka consumer. `run()` may be called once and owns the calling thread. `close()` may be called from
another thread; it stops polling, interrupts active batches, closes the consumer, and waits for the
owner loop. Closing before `run()` closes the deserializers directly. Resources captured by the batch
processor remain application-owned.

`group.id` is required. Automatic commits must be absent or false. `max.poll.records` may be omitted;
if supplied, it must equal `ConsumerProcessingConfig.maxPollRecords()`.

All numeric processing limits must be positive. `maxPollRecords` must not exceed either in-flight
limit so any complete poll response is guaranteed to fit. `maxBatchWait` may be zero; `pollTimeout`
must be positive. Both durations must be representable in nanoseconds.

## Routing and lane ordering

Null keys are rejected. Choose routing according to the deserialized key type:

- `LaneRouter.byKeyHashCode()` for value types such as `String`, records, and well-behaved domain keys.
- `LaneRouter.byByteArrayKey()` for content-based hashing of `byte[]` keys.
- `LaneRouter.byKey(key -> ...)` for an application-defined ordering identity.
- A `LaneRouter` lambda when routing needs the complete `ConsumerRecord`.

The router returns any integer; the library normalizes it to the configured lane count. Equal ordering
identities must return the same value for the lifetime of a setup. Changing the lane count changes the
mapping, which is safe across a full restart but may change which unrelated keys serialize together.

A lane guarantees only ordered, non-overlapping calls to user code. Records are presented in dispatch
order, batch N+1 never overlaps batch N in the same lane, and different lanes may overlap. A batch is
eligible when its lane reaches `maxBatchSize`, or when its oldest queued record reaches
`maxBatchWait`. No thread is permanently assigned to a lane: each batch uses a virtual thread and a
separate configured limit enforces `maxConcurrentBatches`.

## Offsets and failure

Completion is tracked only for offsets actually observed from each partition; offsets are never
assumed contiguous. For observed offsets `100✓, 105✓, 109?, 120✓, 135✓`, the safe commit is `109`.
When `109` completes, the commit may jump to the poll position after `135`.

The processor receives an unmodifiable ordered list. Returning means the whole batch succeeded.
Throwing means the whole setup fails; there is no partial-success contract or built-in retry. Polling,
routing, tracking, and commit failures also fail `run()`. A processor may already have produced some
external effects before failure, so the sink must tolerate replay.

## Bounded buffering

The library tracks both a global in-flight limit and a per-partition in-flight limit. A completed
record behind an earlier incomplete offset still occupies capacity because it remains part of the
uncommitted frontier. A partition is paused whenever another complete `max.poll.records` response
would not fit its limit; all assigned partitions are paused when such a response would not fit the
global limit. Polling continues while partitions are paused so group membership is retained, and
partitions resume when capacity returns.

There is intentionally no per-lane buffer setting. A lane is already bounded by the global and
per-partition limits, and another independent limit would add a second pause policy without bounding
anything the existing limits leave unbounded.

## Rebalances

Revocation or loss discards queued records from the affected assignment. If an active batch contains
even one revoked record, the entire mixed-partition batch is invalidated and its virtual thread is
interrupted. Records in that batch from still-owned partitions are put back at the front of their lane
only after the old invocation returns, preserving non-overlap and order.

Each assignment has an internal epoch. A completion from an older epoch cannot complete offsets,
commit, or affect a later assignment of the same topic partition. The implementation also avoids
committing revoked partitions during or after revocation.

Interruption is cooperative: an HTTP call, JDBC driver, native operation, or user processor may ignore
it or may have already changed downstream state. The library therefore cannot guarantee no cross-JVM
processing overlap after reassignment. Idempotent or otherwise fenced downstream effects are required
when that race matters.

## Shutdown

Shutdown intentionally does not drain queued or active work and does not perform a final commit. It
stops fetching, clears queued state, interrupts active virtual threads, closes the consumer, and
returns after consumer ownership ends. Uncommitted records replay after restart.

Apache Kafka, Kafka, and Apache are trademarks of the Apache Software Foundation. This project is not
affiliated with or endorsed by the Apache Software Foundation.
