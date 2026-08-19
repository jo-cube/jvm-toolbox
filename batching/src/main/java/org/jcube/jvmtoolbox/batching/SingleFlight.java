package org.jcube.jvmtoolbox.batching;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Coalesces concurrent executions for equal keys into one asynchronous operation.
 *
 * <p>A flight is established before one caller starts the operation for a non-null key and ends when
 * the operation throws while starting or its returned stage completes. Equal keys use {@link
 * Object#equals(Object)} and {@link Object#hashCode()}. Different keys never share a flight.
 * Completed results and failures are removed before the coalescer completes caller futures, so a
 * request made from such a completion action starts a new operation. This class does not cache or
 * retry results.
 *
 * <p>Every caller receives an independent, cancellable future. Cancelling one caller's future does not
 * affect the operation or any other caller, even when every caller cancels.
 * Cancellation of the operation's stage completes its callers exceptionally. The operation must not
 * recursively execute the same key through this instance.
 *
 * <p>This class is thread-safe. It creates no threads and does not block except while one caller
 * synchronously starts the operation.
 *
 * @param <K> key type
 * @param <V> result type; result values may be {@code null}
 */
public final class SingleFlight<K, V> {
    private final SingleFlightOperation<K, V> operation;
    private final ConcurrentHashMap<K, Flight<V>> flights = new ConcurrentHashMap<>();

    /**
     * Creates a request coalescer.
     *
     * @param operation asynchronous operation shared by equal concurrent keys
     * @throws NullPointerException if {@code operation} is {@code null}
     */
    public SingleFlight(SingleFlightOperation<K, V> operation) {
        this.operation = Objects.requireNonNull(operation, "operation");
    }

    /**
     * Executes or joins the current flight for a key.
     *
     * <p>Synchronous operation failures and null operation stages complete the returned future
     * exceptionally rather than being thrown from this method.
     *
     * @param key non-null logical operation key
     * @return an independent, cancellable future for this flight
     * @throws NullPointerException if {@code key} is {@code null}
     */
    public CompletableFuture<V> execute(K key) {
        Objects.requireNonNull(key, "key");
        Flight<V> flight = flights.computeIfAbsent(key, ignored -> new Flight<>());
        if (flight.start()) {
            start(key, flight);
        }
        return flight.copy();
    }

    private void start(K key, Flight<V> flight) {
        try {
            var stage = operation.execute(key);
            if (stage == null) {
                throw new IllegalStateException("single-flight operation returned null");
            }
            stage.whenComplete((value, failure) -> finish(key, flight, value, failure));
        } catch (Throwable failure) {
            finish(key, flight, null, failure);
        }
    }

    private void finish(K key, Flight<V> flight, V value, Throwable failure) {
        if (!flights.remove(key, flight)) {
            return;
        }
        if (failure == null) {
            flight.complete(value);
        } else {
            flight.completeExceptionally(failure);
        }
    }

    private static final class Flight<V> extends CompletableFuture<V> {
        private final AtomicBoolean started = new AtomicBoolean();

        private boolean start() {
            return started.compareAndSet(false, true);
        }
    }
}
