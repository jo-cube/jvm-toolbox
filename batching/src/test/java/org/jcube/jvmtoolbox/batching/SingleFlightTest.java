package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SingleFlightTest {
    @Test
    void equalKeysShareOneOperationAndIndependentHandles() throws Exception {
        var calls = new AtomicInteger();
        var operation = new CompletableFuture<String>();
        var singleFlight = new SingleFlight<String, String>(key -> {
            calls.incrementAndGet();
            return operation;
        });

        var first = singleFlight.execute(new String("key"));
        var duplicate = singleFlight.execute(new String("key"));

        assertNotSame(first, duplicate);
        assertEquals(1, calls.get());
        operation.complete("value");
        assertEquals("value", first.get(2, SECONDS));
        assertEquals("value", duplicate.get(2, SECONDS));
    }

    @Test
    void differentKeysDoNotSerializeOperationStartupEvenWhenHashesCollide() throws Exception {
        var bothStarted = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var singleFlight = new SingleFlight<CollisionKey, String>(key -> {
            bothStarted.countDown();
            release.await();
            return CompletableFuture.completedFuture(key.value());
        });
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = callers.submit(() -> singleFlight.execute(new CollisionKey("first")));
            var second = callers.submit(() -> singleFlight.execute(new CollisionKey("second")));

            assertTrue(bothStarted.await(2, SECONDS));
            release.countDown();
            assertEquals("first", first.get(2, SECONDS).get(2, SECONDS));
            assertEquals("second", second.get(2, SECONDS).get(2, SECONDS));
        } finally {
            release.countDown();
        }
    }

    @Test
    void completionRemovesTheFlightBeforeDependentActionsRun() throws Exception {
        var calls = new AtomicInteger();
        var operations = new ArrayList<CompletableFuture<String>>();
        var singleFlight = new SingleFlight<String, String>(key -> {
            calls.incrementAndGet();
            var operation = new CompletableFuture<String>();
            operations.add(operation);
            return operation;
        });

        var followup = singleFlight.execute("key").thenCompose(value -> singleFlight.execute("key"));
        operations.getFirst().complete("first");

        assertEquals(2, calls.get());
        operations.getLast().complete("second");
        assertEquals("second", followup.get(2, SECONDS));
    }

    @Test
    void exceptionalCompletionIsSharedButNotCached() throws Exception {
        var calls = new AtomicInteger();
        var operation = new AtomicReference<>(new CompletableFuture<String>());
        var singleFlight = new SingleFlight<String, String>(key -> {
            calls.incrementAndGet();
            return operation.get();
        });
        var failure = new IOException("failed");

        var first = singleFlight.execute("key");
        var duplicate = singleFlight.execute("key");
        operation.get().completeExceptionally(failure);

        assertSame(failure, failureOf(first));
        assertSame(failure, failureOf(duplicate));
        operation.set(CompletableFuture.completedFuture("recovered"));
        assertEquals("recovered", singleFlight.execute("key").get(2, SECONDS));
        assertEquals(2, calls.get());
    }

    @Test
    void callersCanJoinWhileTheOperationIsStartingAndShareItsSynchronousFailure() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var failure = new IOException("failed to start");
        var singleFlight = new SingleFlight<String, String>(key -> {
            calls.incrementAndGet();
            started.countDown();
            release.await();
            throw failure;
        });
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var firstCall = callers.submit(() -> singleFlight.execute("key"));
            assertTrue(started.await(2, SECONDS));
            var duplicate = singleFlight.execute("key");
            release.countDown();
            var first = firstCall.get(2, SECONDS);

            assertSame(failure, failureOf(first));
            assertSame(failure, failureOf(duplicate));
            assertEquals(1, calls.get());
        } finally {
            release.countDown();
        }
    }

    @Test
    void cancellationAffectsOnlyTheCallingHandle() throws Exception {
        var calls = new AtomicInteger();
        var operation = new CompletableFuture<String>();
        var singleFlight = new SingleFlight<String, String>(key -> {
            calls.incrementAndGet();
            return operation;
        });
        var cancelled = singleFlight.execute("key");
        var alsoCancelled = singleFlight.execute("key");

        assertTrue(cancelled.cancel(true));
        assertTrue(alsoCancelled.cancel(false));
        assertFalse(operation.isCancelled());
        var live = singleFlight.execute("key");
        assertEquals(1, calls.get());

        operation.complete("value");
        assertThrows(CancellationException.class, () -> cancelled.get(2, SECONDS));
        assertThrows(CancellationException.class, () -> alsoCancelled.get(2, SECONDS));
        assertEquals("value", live.get(2, SECONDS));
    }

    @Test
    void operationCancellationFailsCallersAndAllowsANewFlight() throws Exception {
        var calls = new AtomicInteger();
        var operation = new AtomicReference<>(new CompletableFuture<String>());
        var singleFlight = new SingleFlight<String, String>(key -> {
            calls.incrementAndGet();
            return operation.get();
        });
        var first = singleFlight.execute("key");
        var duplicate = singleFlight.execute("key");

        operation.get().cancel(false);

        assertEquals(CancellationException.class, failureOf(first).getClass());
        assertEquals(CancellationException.class, failureOf(duplicate).getClass());
        operation.set(CompletableFuture.completedFuture("next"));
        assertEquals("next", singleFlight.execute("key").get(2, SECONDS));
        assertEquals(2, calls.get());
    }

    @Test
    void nullStageFailsTheFlightWhileNullResultIsAllowed() throws Exception {
        var calls = new AtomicInteger();
        var singleFlight = new SingleFlight<String, String>(key ->
                calls.getAndIncrement() == 0 ? null : CompletableFuture.completedFuture(null));

        var failure = failureOf(singleFlight.execute("key"));
        assertEquals(IllegalStateException.class, failure.getClass());
        assertNull(singleFlight.execute("key").get(2, SECONDS));
        assertEquals(2, calls.get());
    }

    @Test
    void nullOperationAndKeyAreRejected() {
        assertThrows(NullPointerException.class, () -> new SingleFlight<String, String>(null));
        var singleFlight = new SingleFlight<String, String>(key ->
                CompletableFuture.completedFuture(key));
        assertThrows(NullPointerException.class, () -> singleFlight.execute(null));
    }

    @Test
    void highContentionOnOneKeyStartsOneOperation() throws Exception {
        int callerCount = 500;
        var calls = new AtomicInteger();
        var start = new CountDownLatch(1);
        var operation = new CompletableFuture<Integer>();
        var singleFlight = new SingleFlight<String, Integer>(key -> {
            calls.incrementAndGet();
            return operation;
        });
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var results = new ArrayList<java.util.concurrent.Future<CompletableFuture<Integer>>>();
            for (int index = 0; index < callerCount; index++) {
                results.add(callers.submit(() -> {
                    start.await();
                    return singleFlight.execute(new String("key"));
                }));
            }
            start.countDown();

            var handles = new ArrayList<CompletableFuture<Integer>>(callerCount);
            for (var result : results) {
                handles.add(result.get(2, SECONDS));
            }
            assertEquals(1, calls.get());
            operation.complete(42);
            for (var handle : handles) {
                assertEquals(42, handle.get(2, SECONDS));
            }
        }
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        return assertThrows(ExecutionException.class, () -> future.get(2, SECONDS)).getCause();
    }

    private record CollisionKey(String value) {
        @Override
        public int hashCode() {
            return 1;
        }
    }
}
