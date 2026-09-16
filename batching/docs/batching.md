# Batching

`MicroBatcher<I, O>` turns independent foreground operations into positional backend batches. Use it
for writes, bulk RPCs, inference calls, or any operation where equal inputs remain independent. For
lookups where equal keys should share backend work, use [KeyBatchLoader](key-batch-loader.md). When
callers need one incrementally reduced aggregate rather than individual results, use
[WindowedAccumulator](windowed-accumulator.md).

```text
single-item callers -> bounded admission -> batches of List<I>
                                        -> bounded processor calls
                                        -> one future completed per position
```

The batcher is thread-safe. Admission happens synchronously inside `submit`; backend processing and
future completion happen asynchronously after admission.

## Application lifecycle

Create one long-lived batcher for one logical backend operation and share it across requests. A
batcher owns its pending work and coordination lifecycle, but it does not own the processor or
resources captured by the processor.

A typical application lifecycle is:

1. Create the backend client or connection pool.
2. Create the batcher with a processor that uses that backend.
3. Admit foreground requests through the shared batcher.
4. Stop accepting new foreground requests during application shutdown.
5. Close the batcher and allow admitted work to drain.
6. Close the backend client or connection pool.

Do not create a batcher per request. Limits apply to one instance, so account for the combined
concurrency of multiple batchers and application replicas that share a backend.

## Non-keyed backend example

This processor converts independent audit writes into one JDBC batch. Each processor invocation gets
its own connection because processor calls may overlap.

```java
record AuditEvent(long accountId, Instant occurredAt, String message) {}

final class JdbcAuditBackend
        implements BatchProcessor<AuditEvent, Integer> {

    private static final String INSERT = """
            INSERT INTO audit_event(account_id, occurred_at, message)
            VALUES (?, ?, ?)
            """;

    private final DataSource dataSource;

    JdbcAuditBackend(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public List<BatchOutcome<Integer>> process(List<AuditEvent> events)
            throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT)) {

            for (AuditEvent event : events) {
                statement.setLong(1, event.accountId());
                statement.setTimestamp(2, Timestamp.from(event.occurredAt()));
                statement.setString(3, event.message());
                statement.addBatch();
            }

            int[] updateCounts = statement.executeBatch();
            if (updateCounts.length != events.size()) {
                throw new SQLException("JDBC batch returned the wrong number of update counts");
            }
            var outcomes = new ArrayList<BatchOutcome<Integer>>(updateCounts.length);

            for (int position = 0; position < updateCounts.length; position++) {
                int count = updateCounts[position];
                if (count == Statement.EXECUTE_FAILED) {
                    outcomes.add(BatchOutcome.failure(
                            new SQLException("write failed at batch position " + position)));
                } else {
                    outcomes.add(BatchOutcome.success(count));
                }
            }
            return outcomes;
        }
    }
}
```

The returned outcome at position `n` belongs to the input at position `n`. If acquiring the connection,
binding the batch, or executing it throws, every still-live request in that formed batch fails with the
same exception. Transaction boundaries and any interpretation of driver-specific update counts belong
to the application backend.

Create and retain the batcher as application state:

```java
var config = new BatchingConfig(
        128,
        Duration.ofNanos(500_000),
        8,
        16_384,
        AdmissionPolicy.REJECT,
        Duration.ZERO);

var statistics = new BatchStatistics();
var auditWrites = new MicroBatcher<AuditEvent, Integer>(
        config,
        new JdbcAuditBackend(dataSource),
        statistics);
```

`maxConcurrentBatches` bounds overlapping processor calls, not JDBC connections globally. Each
processor invocation above borrows one connection, so reserve enough pool capacity for this batcher
and other database users. Eight replicas with this configuration can collectively issue up to 64
processor calls.

## API-server integration

The following framework-neutral example shows the two phases that a server must handle separately:
synchronous admission and asynchronous completion. `ApiResponse` and `request.onDisconnect` stand in
for the corresponding facilities in the chosen HTTP framework.

```java
CompletionStage<ApiResponse> recordAudit(HttpRequest request) {
    CompletableFuture<Integer> result;
    try {
        result = auditWrites.submit(toAuditEvent(request));
    } catch (RejectedExecutionException overloaded) {
        return CompletableFuture.completedFuture(
                ApiResponse.serviceUnavailable());
    } catch (TimeoutException admissionTimeout) {
        return CompletableFuture.completedFuture(
                ApiResponse.serviceUnavailable());
    } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(interrupted);
    }

    request.onDisconnect(() -> result.cancel(false));

    return result.handle((updateCount, failure) -> {
        if (failure != null) {
            return ApiResponse.internalServerError();
        }
        return ApiResponse.accepted();
    });
}
```

For an event-loop server, normally use `REJECT`: waiting admission would block the event-loop thread
before `submit` returns its future. Translate overload to the response appropriate for the protocol,
commonly HTTP 429 or 503, and apply retries only at a layer that has an explicit retry budget.

For a virtual-thread-per-request server, `WAIT`, timed admission, and `CompletableFuture.get()` are
supported. Blocking virtual threads can simplify application code, while asynchronous future
composition produced higher throughput in this repository's measured workloads.

The API does not specify which thread completes a future. Keep dependent callbacks short and
non-blocking, or use `thenApplyAsync`, `handleAsync`, or the server's response executor for expensive
serialization and further work.

Cancellation on client disconnect is optional application wiring. It prevents an unneeded caller
completion but never interrupts a shared or already-dispatched backend invocation.

## Configuration

`BatchingConfig` is immutable and applies for the lifetime of one batcher.

| Field | Contract | How to choose it |
| --- | --- | --- |
| `maxBatchSize` | Maximum caller submissions in one backend invocation. | Measure the backend directly and choose a useful operating point rather than its largest accepted batch. |
| `maxWait` | Age at which the oldest request in the forming batch makes it eligible. Zero disables intentional linger. | Spend only the portion of the latency budget justified by improved batch fill at low and moderate load. |
| `maxConcurrentBatches` | Maximum processor invocations that may overlap. | Match a measured backend concurrency region and the application's share of the backend resource budget. |
| `maxPendingRequests` | Maximum admitted requests whose work has not retired, including dispatched work. | Treat it as an explicit backend-facing backlog bound, not as the number of connected clients. |
| `admissionPolicy` | Behavior when pending capacity is exhausted. | Choose from the caller execution model and overload policy, not throughput alone. |
| `admissionTimeout` | Positive duration used only by `WAIT_WITH_TIMEOUT`. | Keep it within the caller's remaining deadline. |

Durations are converted to nanoseconds and must be non-negative and representable at that precision.
The maximum wait is measured from the oldest admitted request in the currently forming batch. A batch
becomes eligible when full, when that wait expires, at a flush boundary, or when closing begins.

`maxWait` is an eligibility threshold, not a dispatch deadline. Runtime scheduling and a saturated
backend-concurrency limit may delay actual dispatch.

See [performance](performance.md#applying-the-evidence) for a measurement-first configuration
workflow.

## Admission and overload

Admission occurs inside `submit`:

- `REJECT` throws `RejectedExecutionException` immediately when capacity is exhausted.
- `WAIT` blocks until capacity becomes available or closing begins.
- `WAIT_WITH_TIMEOUT` blocks up to `admissionTimeout`, then throws `TimeoutException`.

All policies reject new work with `RejectedExecutionException` after closing starts. An
`InterruptedException` during admission means the request was not admitted. The library does not
silently drop work, create an unbounded secondary queue, or automatically retry a rejected or failed
request.

`maxPendingRequests` counts all admitted work that has not retired, including work already handed to a
processor. A large number of network connections therefore does not require an equally large pending
limit; the server may keep connections open while the batcher enforces a smaller backend-facing bound.

Once a batch has a validated result or failure, its pending capacity is released before its futures
are completed. This lets a short, non-blocking dependent stage submit follow-up work even when capacity
was full. Dependent stages must still avoid waiting for follow-up work or calling `close()`; use an
asynchronous stage when the continuation may block.

Synchronous dependent actions continue to occupy backend-concurrency slots. At most
`maxBatchSize × maxConcurrentBatches` processed requests can therefore remain in completion delivery
in addition to the configured pending capacity.

## Backend outcomes and failures

`BatchProcessor<I, O>` receives an ordered, unmodifiable input list. Implementations may block, and
processor calls may overlap up to `maxConcurrentBatches`. The processor and captured resources must be
safe for that concurrency. Do not depend on a particular execution thread and do not call back into
the batcher from its processor.

The processor must return one non-null `BatchOutcome<O>` per input in the same order:

| Processor result | Foreground completion |
| --- | --- |
| `BatchOutcome.success(value)` | That position completes normally. A positional value may be `null`. |
| `BatchOutcome.failure(cause)` | Only that position completes exceptionally. |
| Processor throws | Every still-live position in the batch completes exceptionally. |
| Null list, null outcome, or wrong outcome count | The whole batch fails with `IllegalStateException`. |

Equal inputs are independent positions and are never deduplicated. If duplicate-key coalescing and
missing values are required within a formed batch, use `KeyBatchLoader`. If equal keys should share an
operation for its full lifetime without batching, use [`SingleFlight`](single-flight.md).

## Cancellation

Every submission has an independent future:

- Before dispatch, cancelled work is skipped when the coordinator encounters it.
- After dispatch, cancellation does not interrupt or cancel the backend invocation.
- Cancelling one request does not affect sibling positions.
- Cancellation notifies the coordinator, but capacity is released only when cancelled work is
  observed or its dispatched batch retires, not necessarily when `cancel` returns.
- Repeated or concurrent cancellation attempts on the same future produce only one cancellation
  event. As with `CompletableFuture`, `cancel` still returns true for an already-cancelled future.

The batcher owns completion of the returned future. Callers may observe, compose, wait for, or cancel
it, but must not invoke `complete`, `completeExceptionally`, `obtrudeValue`, or `obtrudeException`.

The `mayInterruptIfRunning` argument to `CompletableFuture.cancel` does not interrupt backend work. If
waiting on a returned future is interrupted after admission, processing continues unless the
application separately cancels that future.

## Flush and shutdown

`flush()` makes submissions admitted before its boundary immediately eligible and waits for their
processing and completion delivery without closing admission. Use it at the end of an import chunk or
before a read that depends on earlier writes. Failures remain on the submission futures, so inspect
those results to determine success:

```java
var result = auditWrites.submit(event);
auditWrites.flush();
result.join();
```

The boundary is established under the admission lock. Later submissions can be admitted, but their
batches wait until earlier work finishes and do not delay the flush. Concurrent flushes are supported.
Flushing an empty batcher never invokes the processor; a flush requires no pending capacity and does
not count as a submission in observer statistics. Cancellation skips undispatched work as usual, but
flushing still waits for already-dispatched work even if its callers cancel.

Flush waits uninterruptibly, preserving interrupted status, and has no backend timeout. It also waits
for synchronous completion actions on earlier futures. Do not call it from processor, observer, or
synchronous future callbacks. If closing has already started, it waits for shutdown.

`close()` is graceful, blocking, and idempotent. It stops admission, makes a partial batch immediately
eligible, waits for admitted work and backend invocations to retire, and then returns. New submissions
are rejected once closing starts.

Close waits uninterruptibly and restores the calling thread's interrupted status before returning. It
has no backend timeout, so a processor that never returns can prevent shutdown from completing. Do not
call `close` from processor or observer callbacks.

The batcher does not close its processor, datasource, client, or executor. Stop the server's request
admission first, close batchers second, and close backend resources last.

## Observability

Pass a `BatchObserver` to the observed constructor to receive admission, rejection, cancellation,
dispatch, completion, failure, keyed-coalescing, and closed events. Callbacks are synchronous on the
thread causing each event and callbacks for different work may run concurrently. Observers must be
thread-safe, return promptly, and not call back into the observed batcher. Observer failures are
ignored so they cannot alter request results.

`BatchStatistics` is an optional thread-safe observer. Its immutable snapshots provide cumulative
admitted, rejected, cancelled, dispatched, successful, failed, and keyed/coalescing counters, plus
current incomplete-request and in-flight-batch gauges and the closed state. Snapshots are weakly
consistent while callbacks are concurrent and do not reset counters.

`incompleteRequests` follows terminal observer callbacks rather than internal admission bookkeeping.
Because processed batches release capacity before synchronous future actions finish, replacement
admissions can make this weak gauge temporarily exceed `maxPendingRequests` even though the admission
bound itself remains enforced.

Use the unobserved constructor when instrumentation is not required. It avoids observer callbacks,
statistics updates, and observation-specific timing work.
