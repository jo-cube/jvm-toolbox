package org.jcube.jvmtoolbox.bulkhead;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

/**
 * Bounds concurrent calls to one logical downstream operation.
 *
 * <p>At most the configured number of calls execute at once. When that capacity is occupied, up to
 * the configured number of callers may wait; further callers are rejected. Waiting admission is
 * interruptible and queued callers acquire execution capacity fairly, though thread scheduling may
 * affect the order in which callers reach the queue.
 *
 * <p>Operations run on the admitting caller, so this class creates no threads and owns no executor or
 * other lifecycle. A blocking call retains capacity until it returns or throws. An asynchronous call
 * retains capacity until its supplied stage completes, including exceptional completion or
 * cancellation. Cancelling the returned caller future does not cancel that stage or release capacity
 * early.
 *
 * <p>This class is thread-safe. Operations must not recursively call through the same instance when
 * all execution capacity may already be occupied, because such a call can wait for itself.
 */
public final class Bulkhead {
    private final Semaphore executionPermits;
    private final Semaphore waitingPermits;

    /**
     * Creates a bulkhead.
     *
     * @param maxConcurrentCalls maximum calls executing at once; must be positive
     * @param maxWaitingCallers maximum callers admitted to wait; zero rejects whenever execution
     *     capacity is occupied
     * @throws IllegalArgumentException if either limit is outside its documented range
     */
    public Bulkhead(int maxConcurrentCalls, int maxWaitingCallers) {
        if (maxConcurrentCalls < 1) {
            throw new IllegalArgumentException("maxConcurrentCalls must be positive");
        }
        if (maxWaitingCallers < 0) {
            throw new IllegalArgumentException("maxWaitingCallers must not be negative");
        }
        executionPermits = new Semaphore(maxConcurrentCalls, true);
        waitingPermits = new Semaphore(maxWaitingCallers);
    }

    /**
     * Admits and invokes one blocking operation on the caller thread.
     *
     * @param operation operation to invoke after admission
     * @param <T> result type
     * @return the operation result, which may be {@code null}
     * @throws InterruptedException if interrupted before admission or by the operation
     * @throws RejectedExecutionException if execution and waiting capacity are occupied
     * @throws Exception if the operation throws
     * @throws NullPointerException if {@code operation} is {@code null}
     */
    public <T> T call(Callable<T> operation) throws Exception {
        Objects.requireNonNull(operation, "operation");
        acquire();
        try {
            return operation.call();
        } finally {
            executionPermits.release();
        }
    }

    /**
     * Admits and invokes one blocking operation, waiting no longer than the admission timeout.
     *
     * <p>The timeout covers only waiting for execution capacity, not operation execution. Exhausted
     * waiting capacity still causes immediate rejection.
     *
     * @param admissionTimeout maximum time to wait for execution capacity; must not be negative
     * @param operation operation to invoke after admission
     * @param <T> result type
     * @return the operation result, which may be {@code null}
     * @throws InterruptedException if interrupted before admission or by the operation
     * @throws TimeoutException if the admission timeout expires
     * @throws RejectedExecutionException if execution and waiting capacity are occupied
     * @throws Exception if the operation throws
     * @throws NullPointerException if either argument is {@code null}
     * @throws IllegalArgumentException if {@code admissionTimeout} is negative
     */
    public <T> T call(Duration admissionTimeout, Callable<T> operation) throws Exception {
        Objects.requireNonNull(operation, "operation");
        acquire(admissionTimeout);
        try {
            return operation.call();
        } finally {
            executionPermits.release();
        }
    }

    /**
     * Admits and starts one asynchronous operation on the caller thread.
     *
     * <p>The returned future is an independent caller handle. Cancelling it does not cancel the
     * operation stage or release execution capacity before that stage completes. A synchronous
     * operation-start failure or null stage completes the returned future exceptionally.
     *
     * @param operation operation to start after admission
     * @param <T> result type
     * @return an independent future completed from the operation stage
     * @throws InterruptedException if interrupted before admission
     * @throws RejectedExecutionException if execution and waiting capacity are occupied
     * @throws NullPointerException if {@code operation} is {@code null}
     */
    public <T> CompletableFuture<T> callAsync(
            Callable<? extends CompletionStage<? extends T>> operation) throws InterruptedException {
        Objects.requireNonNull(operation, "operation");
        acquire();
        return start(operation);
    }

    /**
     * Admits and starts one asynchronous operation, waiting no longer than the admission timeout.
     *
     * <p>The timeout covers only waiting for execution capacity, not asynchronous completion.
     * Exhausted waiting capacity still causes immediate rejection. The returned future has the same
     * ownership and cancellation behavior as {@link #callAsync(Callable)}.
     *
     * @param admissionTimeout maximum time to wait for execution capacity; must not be negative
     * @param operation operation to start after admission
     * @param <T> result type
     * @return an independent future completed from the operation stage
     * @throws InterruptedException if interrupted before admission
     * @throws TimeoutException if the admission timeout expires
     * @throws RejectedExecutionException if execution and waiting capacity are occupied
     * @throws NullPointerException if either argument is {@code null}
     * @throws IllegalArgumentException if {@code admissionTimeout} is negative
     */
    public <T> CompletableFuture<T> callAsync(
            Duration admissionTimeout,
            Callable<? extends CompletionStage<? extends T>> operation)
            throws InterruptedException, TimeoutException {
        Objects.requireNonNull(operation, "operation");
        acquire(admissionTimeout);
        return start(operation);
    }

    private void acquire() throws InterruptedException {
        if (acquireImmediatelyOrReserveWaiting()) {
            return;
        }
        try {
            executionPermits.acquire();
        } finally {
            waitingPermits.release();
        }
    }

    private void acquire(Duration timeout) throws InterruptedException, TimeoutException {
        Objects.requireNonNull(timeout, "admissionTimeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("admissionTimeout must not be negative");
        }
        if (acquireImmediatelyOrReserveWaiting()) {
            return;
        }
        try {
            if (!executionPermits.tryAcquire(NANOSECONDS.convert(timeout), NANOSECONDS)) {
                throw new TimeoutException("timed out waiting for bulkhead capacity");
            }
        } finally {
            waitingPermits.release();
        }
    }

    private boolean acquireImmediatelyOrReserveWaiting() throws InterruptedException {
        if (executionPermits.tryAcquire(0, NANOSECONDS)) {
            return true;
        }
        if (waitingPermits.tryAcquire()) {
            return false;
        }
        if (executionPermits.tryAcquire(0, NANOSECONDS)) {
            return true;
        }
        throw new RejectedExecutionException("bulkhead capacity is exhausted");
    }

    private <T> CompletableFuture<T> start(
            Callable<? extends CompletionStage<? extends T>> operation) {
        var result = new CompletableFuture<T>();
        try {
            var stage = operation.call();
            if (stage == null) {
                throw new IllegalStateException("bulkhead operation returned null");
            }
            stage.whenComplete((value, failure) -> {
                executionPermits.release();
                if (failure == null) {
                    result.complete(value);
                } else {
                    result.completeExceptionally(failure);
                }
            });
        } catch (Throwable failure) {
            executionPermits.release();
            result.completeExceptionally(failure);
        }
        return result;
    }
}
