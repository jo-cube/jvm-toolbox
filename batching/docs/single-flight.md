# Single-flight request coalescing

`SingleFlight<K, V>` makes concurrent requests for an equal key share one asynchronous operation. Use
it when duplicate work is expensive but a completed result must never be reused.

```text
caller A ── key 42 ─┐
caller B ── key 42 ─┼── one operation for 42 ── result to A, B, and C
caller C ── key 42 ─┘
caller D ── key 7  ─── one independent operation for 7
```

This is not caching: after the operation for key 42 finishes, the next request for 42 starts another
operation. It is also not batching: the backend still receives one key per invocation.

## Usage

Create one long-lived instance for one logical operation. The key must identify every input that can
change the result.

```java
record UserKey(String tenantId, long userId) {}
record User(String displayName) {}

SingleFlight<UserKey, User> users = new SingleFlight<>(key ->
        userClient.fetchUser(key.tenantId(), key.userId()));

CompletionStage<ApiResponse> getUser(HttpRequest request) {
    var key = new UserKey(request.tenantId(), request.userId());
    CompletableFuture<User> result = users.execute(key);
    request.onDisconnect(() -> result.cancel(false));
    return result.handle((user, failure) ->
            failure == null
                    ? ApiResponse.ok(user)
                    : ApiResponse.internalServerError());
}
```

The operation returns a `CompletionStage` and may throw while starting. `execute` invokes it on one
caller's thread, so it should start asynchronous work and return promptly. `SingleFlight`
creates no executor or worker thread and does not close the operation or anything it captures.

## Flight boundary and concurrency

A flight is established before one caller starts its operation. Until that flight ends, every caller
whose key is equal according to `equals` and `hashCode` joins it. Calls for different keys use
independent map entries and may start their operations concurrently, including when their hash codes
collide.

A flight ends when the operation throws while starting, returns a null stage, or the completion action
for its returned stage runs. The entry is conditionally removed by both key and flight identity before
caller futures are completed. Consequently:

- a caller that joins the current entry shares its success or failure;
- a request made from a completion action triggered by the shared outcome starts a new flight;
- late completion work from an old flight cannot remove a newer flight for the same key;
- no completed value or failure remains available for reuse.

The operation must not recursively call the same `SingleFlight` instance for the same key: the nested
call joins the operation whose stage it is trying to produce. Keys must remain stable under `equals`
and `hashCode` while in flight, as required for any concurrent-map key.

## Results, failure, and cancellation

Every caller gets a distinct `CompletableFuture` view of the shared outcome. Successful values,
including null, are fanned out to all live callers. An exception thrown while starting or an
exceptional stage completion fails every live caller with that cause. Returning a null stage is a
contract violation and fails the flight with `IllegalStateException`. Null operations and keys are
rejected synchronously with `NullPointerException`.

Cancelling a caller future affects only that handle. It never cancels the operation, removes the
flight, or affects another caller; this remains true if every current caller cancels. A later caller
still joins the operation while it remains in flight. The `mayInterruptIfRunning` argument has no
effect on the shared operation.

`SingleFlight` does not own the operation stage. If its external owner cancels that stage, live caller
futures complete exceptionally with `CancellationException` as their cause, the entry is removed, and
the next request may start a new flight. The primitive never decides that an abandoned operation is
safe to stop.

## Scope and lifecycle

There is no `close` method because the primitive owns no threads or resources. A never-completing
operation necessarily retains its key and flight so later callers can join it; use bounded downstream
timeouts where operations may otherwise never finish. Terminal operations are removed promptly by
their completion action.

The primitive deliberately has no cache, expiry, retries, admission limit, queue, batch window,
executor, observer, or tuning options. Put those policies at the layer that owns them. Use
[`KeyBatchLoader`](key-batch-loader.md) when a backend can fetch a set of keys together and bounded
admission is required; use [`MicroBatcher`](batching.md) for positional bulk operations.
