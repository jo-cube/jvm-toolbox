# Windowed accumulation

`WindowedAccumulator<I, A>` incrementally combines many contributions into one accumulator state and
passes each completed state to one downstream action. Use it for counters, statistics, histograms,
buffers, and summaries where callers do not need individual results.

```text
contributions -> one mutable state -> count/time/flush boundary -> one processor call
```

This is intentionally different from request batching:

| Need | Primitive |
| --- | --- |
| Retain every input, invoke a bulk backend, and complete one future per input | `MicroBatcher` |
| Coalesce equal lookup keys within a request batch | `KeyBatchLoader` |
| Incrementally reduce inputs and perform one action per completed aggregate | `WindowedAccumulator` |

The accumulator keeps no separate input reference or completion object after the accumulation callback
returns. It is not a stream processor: there are no event-time, sliding, session, watermark, retry, or
policy-extension semantics.

## Example

This counter emits a total after 1,000 contributions or 250 milliseconds from the first contribution,
whichever boundary is reached first:

```java
var config = new WindowedAccumulatorConfig(
        1_000,
        Duration.ofMillis(250),
        10_000,
        AdmissionPolicy.WAIT_WITH_TIMEOUT,
        Duration.ofMillis(100));

var totals = new WindowedAccumulator<Long, long[]>(
        config,
        () -> new long[1],
        (total, value) -> total[0] += value,
        total -> summaryStore.write(total[0]));

totals.add(4L);
totals.add(7L);
totals.flush();
```

Use an application state type instead of an array when the aggregate has several fields. Include a
count or timestamps in that state if the downstream action needs them; the primitive does not add
window metadata.

## Window boundary and ordering

One instance has one forming window. Successful `add` calls are serialized by a lock, which gives
contributions a total admission order. A non-empty window is completed by the first applicable event:

- its contribution count reaches `maxInputsPerWindow`;
- its age reaches `maxWait`;
- `flush()` rotates it; or
- `close()` starts draining.

The timer starts at the first successful contribution to an empty window. Later contributions do not
reset it. Admission checks an expired boundary before accumulating, so a contribution admitted at or
after the deadline starts the next window even if the coordinator has not yet run. Runtime scheduling
may delay processor invocation; `maxWait` defines window eligibility, not an end-to-end processing
deadline. `Duration.ZERO` therefore makes every contribution its own window.

Completed states are processed serially in window order. While one processor call is running, callers
may form and complete later windows. Those states wait in order and remain covered by
`maxPendingInputs`. Serial processing keeps summary side effects ordered and avoids another concurrency
tuning surface; partition independent work across several accumulator instances when measurement
shows one processor is insufficient.

Empty windows are never created or emitted.

## Accumulation and state ownership

The factory is called on the admitting caller when the first contribution for a new window arrives.
It must return a distinct, non-null mutable state. The accumulation callback then runs on admitting
caller threads, one at a time under the window lock. It should be short and non-blocking because a slow
callback delays every producer.

After rotation, the state is transferred directly to `WindowProcessor` asynchronously on a
library-owned thread. There is no snapshot or copy, and the accumulator never accesses that state
again. The processor has exclusive ownership and may mutate, retain, or transfer it. Factory and
accumulation callbacks must not retain or otherwise access states outside their invocation. Processor
code must not depend on the identity or kind of its invocation thread.

This ownership model makes mutable counters and buffers allocation-efficient. If an accumulation
callback deliberately stores an input reference inside the state, that retention belongs to the
application rather than the primitive.

## Admission and backpressure

`maxPendingInputs` counts every admitted contribution until its completed state's processor call
returns. It covers the forming state, queued completed states, and the state currently being processed.
It bounds logical backlog without retaining a separate object per input.

When the limit is reached:

- `REJECT` throws `RejectedExecutionException` immediately;
- `WAIT` blocks until processing releases capacity or closing begins; and
- `WAIT_WITH_TIMEOUT` blocks up to `admissionTimeout`, then throws `TimeoutException`.

Interruption while acquiring the lock or waiting for capacity means the contribution was not admitted.
New contributions are rejected once closing or terminal failure begins. No fairness ordering is
promised among waiting producers.

## Flush, close, and concurrent contributions

`flush()` rotates the current window under the same lock used by `add`, then waits for it and every
earlier window to finish processing. Contributions admitted before that lock boundary are included;
later contributions belong to the next window and do not delay the flush. Calling `flush()` with no
new contributions creates no state, though it still waits for earlier processing.

`close()` is blocking, graceful, and idempotent. It stops admission, rotates a partial window, drains
all admitted states, and returns. Both methods wait uninterruptibly and restore the caller's interrupted
status. They have no processor timeout, so a processor that never returns prevents them from
returning. Do not call `add`, `flush`, or `close` from factory, accumulator, or processor callbacks.

The accumulator owns its coordination threads but does not own or close callback objects or resources
they capture. Stop external admission first, close the accumulator second, then close downstream
resources.

## Failure and cancellation

A factory or accumulation exception is synchronous and that contribution is not admitted. If an
accumulation callback modifies an existing state before throwing, it must undo that mutation before
throwing; the primitive cannot copy or roll back arbitrary mutable state.

If a processor throws, that window is not retried. Admission stops, but every state already admitted is
still passed to the processor once in order. The first processor failure is exposed as the cause of:

- `RejectedExecutionException` from later `add` calls; and
- `IllegalStateException` from `flush()` and `close()` after admitted work drains.

Processing may have produced a partial external effect before throwing, so idempotence and recovery
belong to the downstream operation.

There is no contribution cancellation: `add` has no per-input handle, and successful accumulation is
immediately part of shared state. Use `MicroBatcher` when each caller needs an independently cancellable
completion.
