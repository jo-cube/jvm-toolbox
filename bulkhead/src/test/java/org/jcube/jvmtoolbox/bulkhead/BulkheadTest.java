package org.jcube.jvmtoolbox.bulkhead;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BulkheadTest {
    @Test
    void rejectsInvalidLimits() {
        assertThrows(IllegalArgumentException.class, () -> new Bulkhead(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new Bulkhead(1, -1));
    }

    @Test
    void neverExceedsTheConcurrentCallLimitUnderContention() throws Exception {
        int callCount = 128;
        int concurrency = 4;
        var bulkhead = new Bulkhead(concurrency, callCount - concurrency);
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        var allSlotsOccupied = new CountDownLatch(concurrency);
        var release = new CountDownLatch(1);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<java.util.concurrent.Future<Integer>>(callCount);
            for (int value = 0; value < callCount; value++) {
                int result = value;
                results.add(callers.submit(() -> bulkhead.call(() -> {
                    int current = active.incrementAndGet();
                    maximum.accumulateAndGet(current, Math::max);
                    allSlotsOccupied.countDown();
                    try {
                        release.await();
                        return result;
                    } finally {
                        active.decrementAndGet();
                    }
                })));
            }

            assertTrue(allSlotsOccupied.await(2, SECONDS));
            assertEquals(concurrency, active.get());
            release.countDown();
            for (int index = 0; index < results.size(); index++) {
                assertEquals(index, results.get(index).get(2, SECONDS));
            }
            assertEquals(concurrency, maximum.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void boundsWaitingCallersAndResumesThemAfterCapacityReturns() throws Exception {
        var bulkhead = new Bulkhead(1, 1);
        var activeStarted = new CountDownLatch(1);
        var releaseActive = new CountDownLatch(1);
        var first = Thread.ofVirtual().start(() -> uncheckedCall(bulkhead, () -> {
            activeStarted.countDown();
            releaseActive.await();
            return "first";
        }));
        assertTrue(activeStarted.await(2, SECONDS));

        var waitingResult = new CompletableFuture<String>();
        var waiter = Thread.ofVirtual().start(() -> completeFromCall(waitingResult, bulkhead, () -> "second"));
        awaitWaiting(waiter);

        assertThrows(RejectedExecutionException.class, () -> bulkhead.call(() -> "rejected"));
        releaseActive.countDown();
        assertEquals("second", waitingResult.get(2, SECONDS));
        first.join();
        waiter.join();
    }

    @Test
    void timeoutDoesNotRunTheOperationOrLeakWaitingCapacity() throws Exception {
        var bulkhead = new Bulkhead(1, 1);
        var source = new CompletableFuture<String>();
        var occupied = bulkhead.callAsync(() -> source);
        var called = new AtomicBoolean();

        assertThrows(TimeoutException.class, () -> bulkhead.call(Duration.ZERO, () -> {
            called.set(true);
            return "unexpected";
        }));
        assertThrows(TimeoutException.class, () -> bulkhead.callAsync(Duration.ZERO, () -> {
            called.set(true);
            return CompletableFuture.completedFuture("unexpected");
        }));
        assertFalse(called.get());

        var replacement = new CompletableFuture<String>();
        var waiter = Thread.ofVirtual().start(() -> completeFromCall(replacement, bulkhead, () -> "replacement"));
        awaitWaiting(waiter);
        source.complete("occupied");

        assertEquals("occupied", occupied.get(2, SECONDS));
        assertEquals("replacement", replacement.get(2, SECONDS));
        waiter.join();
    }

    @Test
    void interruptionBeforeAdmissionDoesNotRunTheOperationOrLeakWaitingCapacity() throws Exception {
        var bulkhead = new Bulkhead(1, 1);
        var source = new CompletableFuture<String>();
        bulkhead.callAsync(() -> source);
        var called = new AtomicBoolean();
        var outcome = new CompletableFuture<Throwable>();
        var waiter = Thread.ofVirtual().start(() -> {
            try {
                bulkhead.call(() -> {
                    called.set(true);
                    return "unexpected";
                });
                outcome.complete(new AssertionError("interrupted call was admitted"));
            } catch (Throwable failure) {
                outcome.complete(failure);
            }
        });
        awaitWaiting(waiter);

        waiter.interrupt();
        assertInstanceOf(InterruptedException.class, outcome.get(2, SECONDS));
        assertFalse(called.get());
        waiter.join();

        var replacement = new CompletableFuture<String>();
        var replacementWaiter = Thread.ofVirtual().start(
                () -> completeFromCall(replacement, bulkhead, () -> "replacement"));
        awaitWaiting(replacementWaiter);
        source.complete("done");
        assertEquals("replacement", replacement.get(2, SECONDS));
        replacementWaiter.join();
    }

    @Test
    void blockingFailureAlwaysReturnsCapacity() throws Exception {
        var bulkhead = new Bulkhead(1, 0);
        var failure = new IOException("failed");

        assertSame(failure, assertThrows(IOException.class, () -> bulkhead.call(() -> {
            throw failure;
        })));
        assertEquals("recovered", bulkhead.call(() -> "recovered"));
    }

    @Test
    void asynchronousCompletionReturnsCapacityBeforeCompletingTheCallerFuture() throws Exception {
        var bulkhead = new Bulkhead(1, 0);
        var source = new CompletableFuture<String>();
        var result = bulkhead.callAsync(() -> source);
        var followup = result.thenCompose(ignored -> {
            try {
                return bulkhead.callAsync(() -> CompletableFuture.completedFuture("second"));
            } catch (InterruptedException failure) {
                return CompletableFuture.failedFuture(failure);
            }
        });

        assertThrows(RejectedExecutionException.class, () -> bulkhead.call(() -> "rejected"));
        source.complete("first");

        assertEquals("first", result.get(2, SECONDS));
        assertEquals("second", followup.get(2, SECONDS));
    }

    @Test
    void cancellingTheCallerFutureDoesNotCancelWorkOrReleaseCapacityEarly() throws Exception {
        var bulkhead = new Bulkhead(1, 0);
        var source = new CompletableFuture<String>();
        var result = bulkhead.callAsync(() -> source);

        assertTrue(result.cancel(true));
        assertFalse(source.isCancelled());
        assertThrows(RejectedExecutionException.class, () -> bulkhead.call(() -> "too early"));

        source.complete("ignored");
        assertEquals("next", bulkhead.call(() -> "next"));
    }

    @Test
    void asynchronousFailureAndOperationCancellationReturnCapacity() throws Exception {
        var bulkhead = new Bulkhead(1, 0);
        var failure = new IOException("failed");
        var failedSource = new CompletableFuture<String>();
        var failed = bulkhead.callAsync(() -> failedSource);

        failedSource.completeExceptionally(failure);
        assertSame(failure, failureOf(failed));

        var cancelledSource = new CompletableFuture<String>();
        var cancelled = bulkhead.callAsync(() -> cancelledSource);
        cancelledSource.cancel(false);
        assertThrows(CancellationException.class, () -> cancelled.get(2, SECONDS));

        assertEquals("next", bulkhead.call(() -> "next"));
    }

    @Test
    void asynchronousStartupFailuresAndNullStagesReturnCapacity() throws Exception {
        var bulkhead = new Bulkhead(1, 0);
        var failure = new IOException("failed to start");

        var failed = bulkhead.<String>callAsync(() -> {
            throw failure;
        });
        assertSame(failure, failureOf(failed));

        var invalid = bulkhead.<String>callAsync(() -> null);
        assertInstanceOf(IllegalStateException.class, failureOf(invalid));

        var recovered = bulkhead.callAsync(() -> CompletableFuture.completedFuture("recovered"));
        assertEquals("recovered", recovered.get(2, SECONDS));
    }

    @Test
    void invalidDurationsAndNullOperationsAreRejectedBeforeAdmission() {
        var bulkhead = new Bulkhead(1, 0);

        assertThrows(IllegalArgumentException.class, () -> bulkhead.call(Duration.ofNanos(-1), () -> null));
        assertThrows(NullPointerException.class, () -> bulkhead.call((java.util.concurrent.Callable<Object>) null));
        assertThrows(NullPointerException.class, () -> bulkhead.callAsync(null));
    }

    private static void awaitWaiting(Thread thread) {
        long deadline = System.nanoTime() + SECONDS.toNanos(2);
        while (thread.getState() != Thread.State.WAITING) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("thread did not wait for bulkhead capacity");
            }
            Thread.onSpinWait();
        }
    }

    private static <T> void completeFromCall(
            CompletableFuture<T> result, Bulkhead bulkhead, java.util.concurrent.Callable<T> operation) {
        try {
            result.complete(bulkhead.call(operation));
        } catch (Throwable failure) {
            result.completeExceptionally(failure);
        }
    }

    private static void uncheckedCall(Bulkhead bulkhead, java.util.concurrent.Callable<?> operation) {
        try {
            bulkhead.call(operation);
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        return assertThrows(ExecutionException.class, () -> future.get(2, SECONDS)).getCause();
    }
}
