# Bulkhead

`Bulkhead` protects one finite downstream resource by bounding concurrent calls and, optionally,
callers waiting for execution capacity.

```text
callers -> at most maxWaitingCallers waiting -> at most maxConcurrentCalls running -> downstream
```

Create one instance for one logical downstream capacity budget and share it across callers:

```java
var databaseCalls = new Bulkhead(16, 64);

Customer customer = databaseCalls.call(() -> repository.load(customerId));
```

When all 16 calls are active, up to 64 callers wait interruptibly. The next caller receives
`RejectedExecutionException`. Set `maxWaitingCallers` to zero for immediate rejection whenever all
execution slots are occupied. A timed call limits admission waiting only:

```java
Customer customer = databaseCalls.call(
        Duration.ofMillis(20),
        () -> repository.load(customerId));
```

Expiration throws `TimeoutException`; interruption before admission throws `InterruptedException` and
does not start the operation. Pending callers acquire execution capacity fairly once queued, without
busy waiting. Arrival order is not guaranteed because thread scheduling determines when a caller
reaches that queue. `Duration.ZERO` provides per-call non-blocking admission when waiting capacity is
configured for other callers.

## Asynchronous operations

`callAsync` starts the operation on the admitting caller and retains its execution slot until the
returned stage completes:

```java
CompletableFuture<HttpResponse<String>> response = httpCalls.callAsync(
        () -> client.sendAsync(request, BodyHandlers.ofString()));
```

Admission is still synchronous and may wait, reject, time out, or be interrupted before
`callAsync` returns. Use zero waiting capacity on event-loop threads; interruptible waiting fits
virtual-thread-per-request code. Because no caller future exists before admission completes,
interrupting the waiting caller is how work is abandoned before admission.

The returned future is an independent caller handle. Cancelling it does not cancel the downstream
stage and does not release capacity early. Capacity is released when that stage succeeds, fails, or is
itself cancelled. A synchronous startup failure or null stage fails the caller future and releases
capacity.

## Scope and ownership

`Bulkhead` invokes work on caller threads. It creates no threads, owns no executor, queues no task
objects, and needs no `close`. Blocking-operation exceptions propagate unchanged; asynchronous
operation outcomes complete the returned future. Capacity is released before dependent completion
actions run.

Do not recursively call through the same instance when its execution slots may all be occupied: the
nested call can wait for its outer call. Split independent downstream capacity budgets into separate
instances.

This abstraction differs from adjacent tools:

- An `ExecutorService` chooses where and when tasks run; `Bulkhead` only admits calls on their current
  threads.
- A `Semaphore` exposes permits that callers can leak; `Bulkhead` owns release across normal, failure,
  interruption, timeout, and asynchronous completion paths.
- A rate limiter bounds starts over time; `Bulkhead` bounds work that is currently incomplete.
- Retry, timeout of running work, circuit breaking, scheduling, and policy composition remain the
  application's responsibility.
