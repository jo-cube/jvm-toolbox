package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class MicroBatcherConcurrencyTest {
    @Test
    void neverExceedsTheConcurrentBatchLimit() throws Exception {
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        var twoStarted = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var config = config(1, 2, 4);
        var batcher = new MicroBatcher<Integer, Integer>(config, inputs -> {
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            twoStarted.countDown();
            release.await();
            active.decrementAndGet();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        });
        try {
            var results = List.of(
                    batcher.submit(1), batcher.submit(2), batcher.submit(3), batcher.submit(4));

            assertTrue(twoStarted.await(2, SECONDS));
            assertEquals(2, active.get());
            assertEquals(2, maximum.get());
            release.countDown();
            for (int index = 0; index < results.size(); index++) {
                assertEquals(index + 1, results.get(index).get(2, SECONDS));
            }
        } finally {
            release.countDown();
            batcher.close();
        }
    }

    @Test
    void cancellationBeforeDispatchDoesNotAffectSiblings() throws Exception {
        var clock = new AtomicInteger();
        var received = new ArrayList<List<String>>();
        var config = new BatchingConfig(
                2,
                Duration.ofHours(1),
                1,
                4,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
        try (var batcher = new MicroBatcher<String, String>(config, inputs -> {
            synchronized (received) {
                received.add(inputs);
            }
            return inputs.stream().map(BatchOutcome::success).toList();
        }, () -> clock.get() * Duration.ofHours(1).toNanos())) {
            var cancelled = batcher.submit("cancelled");
            cancelled.cancel(false);
            var live = batcher.submit("live");
            clock.incrementAndGet();
            batcher.signalCoordinator();

            assertThrows(CancellationException.class, () -> cancelled.get(2, SECONDS));
            assertEquals("live", live.get(2, SECONDS));
            synchronized (received) {
                assertEquals(List.of(List.of("live")), received);
            }
        }
    }

    @Test
    void cancellationAfterDispatchDoesNotCancelTheSharedBatch() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var config = config(2, 1, 2);
        var batcher = new MicroBatcher<String, String>(config, inputs -> {
            started.countDown();
            release.await();
            return inputs.stream().map(BatchOutcome::success).toList();
        });
        try {
            var cancelled = batcher.submit("cancelled");
            var live = batcher.submit("live");
            assertTrue(started.await(2, SECONDS));

            cancelled.cancel(false);
            release.countDown();

            assertThrows(CancellationException.class, () -> cancelled.get(2, SECONDS));
            assertEquals("live", live.get(2, SECONDS));
        } finally {
            release.countDown();
            batcher.close();
        }
    }

    @Test
    void gracefulCloseFlushesAPartialBatchAndRejectsLaterWork() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var config = new BatchingConfig(
                3,
                Duration.ofHours(1),
                1,
                3,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
        var batcher = new MicroBatcher<String, String>(config, inputs -> {
            started.countDown();
            release.await();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        });
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var accepted = batcher.submit("accepted");
            var close = callers.submit(batcher::close);
            assertTrue(started.await(2, SECONDS));
            assertThrows(RejectedExecutionException.class, () -> batcher.submit("late"));

            release.countDown();
            assertEquals("accepted", accepted.get(2, SECONDS));
            close.get(2, SECONDS);
        } finally {
            release.countDown();
            batcher.close();
        }
    }

    @Test
    void interruptingSubmitAndWaitDoesNotCancelAdmittedWork() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var callerInterrupted = new CountDownLatch(1);
        var unexpected = new AtomicReference<Throwable>();
        var statistics = new BatchStatistics();
        var batcher = new MicroBatcher<String, String>(config(1, 1, 1), inputs -> {
            backendStarted.countDown();
            releaseBackend.await();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        }, statistics);
        try {
            Thread caller = Thread.ofVirtual().start(() -> {
                try {
                    batcher.submitAndWait("value");
                    unexpected.set(new AssertionError("blocking wait completed without interruption"));
                } catch (InterruptedException expected) {
                    callerInterrupted.countDown();
                } catch (Throwable failure) {
                    unexpected.set(failure);
                }
            });
            assertTrue(backendStarted.await(2, SECONDS));

            caller.interrupt();
            assertTrue(callerInterrupted.await(2, SECONDS));
            caller.join();
            assertNull(unexpected.get());
            assertEquals(1, statistics.snapshot().incompleteRequests());

            releaseBackend.countDown();
            batcher.close();
            assertEquals(0, statistics.snapshot().cancelledRequests());
            assertEquals(1, statistics.snapshot().successfulRequests());
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
    }

    @Test
    void closeDrainsAndRestoresTheCallersInterruptedStatus() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var closeReturned = new CountDownLatch(1);
        var interruptedAfterClose = new AtomicInteger();
        var batcher = new MicroBatcher<String, String>(config(1, 1, 1), inputs -> {
            backendStarted.countDown();
            releaseBackend.await();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        });
        try {
            var result = batcher.submit("value");
            assertTrue(backendStarted.await(2, SECONDS));
            Thread closer = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                batcher.close();
                interruptedAfterClose.set(Thread.currentThread().isInterrupted() ? 1 : 0);
                closeReturned.countDown();
            });

            assertFalse(closeReturned.await(50, java.util.concurrent.TimeUnit.MILLISECONDS));
            releaseBackend.countDown();

            assertEquals("value", result.get(2, SECONDS));
            assertTrue(closeReturned.await(2, SECONDS));
            closer.join();
            assertEquals(1, interruptedAfterClose.get());
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
    }

    @RepeatedTest(20)
    void concurrentSubmissionsDoNotStrandCallers() throws Exception {
        var config = config(7, 3, 200);
        try (var batcher = new MicroBatcher<Integer, Integer>(config, inputs ->
                        inputs.stream().map(BatchOutcome::success).toList());
                var producers = Executors.newVirtualThreadPerTaskExecutor()) {
            var submitted = new ArrayList<java.util.concurrent.Future<Integer>>();
            for (int value = 0; value < 100; value++) {
                int input = value;
                submitted.add(producers.submit(() -> batcher.submit(input).get(2, SECONDS)));
            }
            for (int index = 0; index < submitted.size(); index++) {
                assertEquals(index, submitted.get(index).get(2, SECONDS));
            }
        }
    }

    private static BatchingConfig config(int maxBatchSize, int maxConcurrentBatches, int capacity) {
        return new BatchingConfig(
                maxBatchSize,
                Duration.ofMillis(1),
                maxConcurrentBatches,
                capacity,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
    }
}
