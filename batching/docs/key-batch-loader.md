# Key batch loading

`KeyBatchLoader<K, V>` is the lookup-oriented specialization of the same batching runtime used by
`MicroBatcher`. Foreground callers load one key and receive `CompletableFuture<Optional<V>>`; the
backend receives a set of unique keys.

Use it when the backend can fetch several keys together and equal keys should share work within one
batching window. Use [MicroBatcher](batching.md) when requests are positional operations that must
remain independent.

```text
logical callers:   A  A  B  C  A
                         │
formed backend set:      A  B  C
                         │
completion fan-out: A -> every still-live A caller
```

## Coalescing and cache scope

Equal keys in one formed batch are sent to the `KeyBatchProcessor` once. One backend outcome is fanned
out to every still-live caller for that key.

Coalescing ends at dispatch. A request admitted into a later batching window can invoke the backend
again even while an earlier batch is running. The loader has no persistent cache and does not reuse
completed values.

`maxBatchSize` and `maxPendingRequests` count caller submissions, not unique keys. One hundred callers
for the same key consume one hundred pending slots even if the backend sees that key once. This makes
capacity independent of duplicate rate and bounds retained caller state.

## PostgreSQL backend example

The following processor uses PostgreSQL `UNNEST` for a composite `(text, integer)` key. It copies the
set into one list so the two bound arrays retain positional correspondence.

```java
record UserKey(String tenantId, int userId) {}
record User(String displayName) {}

final class PostgresUserBackend
        implements KeyBatchProcessor<UserKey, User> {

    private static final String LOOKUP = """
            SELECT stored.tenant_id, stored.user_id, stored.display_name
            FROM unnest(?::text[], ?::integer[])
                    AS requested(tenant_id, user_id)
            JOIN app_user AS stored USING (tenant_id, user_id)
            """;

    private final DataSource dataSource;

    PostgresUserBackend(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Map<UserKey, BatchOutcome<User>> load(Set<UserKey> keys)
            throws SQLException {
        var orderedKeys = List.copyOf(keys);
        String[] tenants = orderedKeys.stream()
                .map(UserKey::tenantId)
                .toArray(String[]::new);
        Integer[] userIds = orderedKeys.stream()
                .map(UserKey::userId)
                .toArray(Integer[]::new);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(LOOKUP)) {

            Array tenantArray = connection.createArrayOf("text", tenants);
            Array userIdArray = null;
            try {
                userIdArray = connection.createArrayOf("integer", userIds);
                statement.setArray(1, tenantArray);
                statement.setArray(2, userIdArray);

                var outcomes = new HashMap<UserKey, BatchOutcome<User>>();
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        var key = new UserKey(rows.getString(1), rows.getInt(2));
                        var user = new User(rows.getString(3));
                        outcomes.put(key, BatchOutcome.success(user));
                    }
                }
                return outcomes;
            } finally {
                tenantArray.free();
                if (userIdArray != null) {
                    userIdArray.free();
                }
            }
        }
    }
}
```

Requested keys absent from the query result are deliberately omitted from the returned map; the loader
turns those omissions into `Optional.empty()`. Every overlapping processor invocation acquires its own
connection. Do not share a `Connection`, `PreparedStatement`, or mutable result map across invocations.

Create one long-lived loader as application state:

```java
var users = new KeyBatchLoader<UserKey, User>(
        config,
        new PostgresUserBackend(dataSource),
        statistics);
```

Processor calls may overlap up to `maxConcurrentBatches`. Size the datasource and the deployment-wide
database budget for this loader, other database users, other loader instances, and all application
replicas. The loader itself enforces only its local configured limit.

## API-server integration

Admission errors occur before `load` returns; lookup results and backend failures arrive through its
future. This framework-neutral example shows the distinction:

```java
CompletionStage<ApiResponse> getUser(HttpRequest request) {
    CompletableFuture<Optional<User>> result;
    try {
        result = users.load(new UserKey(request.tenantId(), request.userId()));
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

    return result.handle((user, failure) -> {
        if (failure != null) {
            return ApiResponse.internalServerError();
        }
        return user.map(ApiResponse::ok)
                .orElseGet(ApiResponse::notFound);
    });
}
```

`ApiResponse` and `request.onDisconnect` represent the chosen server framework's response and
disconnect APIs. Event-loop servers should normally use `REJECT`; virtual-thread handlers may use
waiting admission. See [API-server integration](batching.md#api-server-integration) for the threading,
overload, callback, retry, and blocking-consumption guidance shared by both abstractions.

## Backend result contract

`KeyBatchProcessor<K, V>` receives an unmodifiable set of unique, non-null requested keys. It may be
called concurrently and must return a map whose keys are a subset of that set.

| Backend result | Caller completion |
| --- | --- |
| Requested key mapped to `BatchOutcome.success(value)` | `Optional.of(value)` |
| Requested key omitted from the map | `Optional.empty()` |
| Requested key mapped to `BatchOutcome.failure(cause)` | Exceptional completion with `cause` |
| Processor throws | Every still-live caller represented by the batch fails |

Mapped outcomes and successful values must be non-null. Returning an unrequested key, null map, null
outcome, or successful null value is a contract violation that fails the whole batch. Missing is
therefore structurally distinct from null and failure.

The loader does not own or close its processor, datasource, or other captured resources. Processor
code must not call back into the loader that invoked it.

## Cancellation, capacity, and shutdown

Every caller receives an independent future. Cancelling one duplicate caller does not cancel another
caller or remove a backend key required by another live caller. If every caller for a key is cancelled
before dispatch, its work can be skipped; cancellation after dispatch never interrupts the backend.

Cancellation notifies the batching coordinator. Cancelled callers release capacity only when the
coordinator observes them or when their already-dispatched batch retires, so cancellation is not an
immediate capacity reservation mechanism.

Admission policies, timing, observer behavior, and graceful shutdown are identical to `MicroBatcher`.
During shutdown, stop new frontend requests, close the loader so admitted work drains, and close its
backend resources last. See [batching](batching.md#admission-and-overload) for the shared contracts.
