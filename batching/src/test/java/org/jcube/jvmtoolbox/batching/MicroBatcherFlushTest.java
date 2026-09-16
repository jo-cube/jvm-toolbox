package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;

class MicroBatcherFlushTest {
    @Test
    void flushNeedsNoCapacityAndDoesNotAdmitOrDispatchEmptyWork() throws Exception {
        var statistics = new BatchStatistics();
        var batches = new LinkedBlockingQueue<List<String>>();
        var batcher = new MicroBatcher<String, String>(config(4, 1, 1), inputs -> {
            batches.add(inputs);
            return inputs.stream().map(BatchOutcome::success).toList();
        }, statistics);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                callers.submit(batcher::flush).get(2, SECONDS);
                var first = batcher.submit("first");
                callers.submit(batcher::flush).get(2, SECONDS);
                assertEquals("first", first.join());
                var second = batcher.submit("second");
                callers.submit(batcher::flush).get(2, SECONDS);
                assertEquals("second", second.join());
                assertEquals(List.of(List.of("first"), List.of("second")), List.copyOf(batches));
                assertEquals(2, statistics.snapshot().admittedRequests());
                assertEquals(2, statistics.snapshot().dispatchedRequests());
                assertEquals(0, statistics.snapshot().incompleteRequests());
            } finally {
                batcher.close();
            }
            callers.submit(batcher::flush).get(2, SECONDS);
        }
    }

    @Test
    void boundaryWaitsForOutOfOrderBatchesAndExcludesLaterSubmissions() throws Exception {
        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var partialStarted = new CountDownLatch(1);
        var batcher = new MicroBatcher<String, String>(config(2, 2, 4), inputs -> {
            if (inputs.contains("first")) {
                firstStarted.countDown();
                releaseFirst.await();
            } else if (inputs.contains("partial")) {
                partialStarted.countDown();
            }
            return inputs.stream().map(BatchOutcome::success).toList();
        }, () -> 0);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var first = batcher.submit("first");
                var sibling = batcher.submit("sibling");
                assertTrue(firstStarted.await(2, SECONDS));
                var partial = batcher.submit("partial");
                var flush = callers.submit(batcher::flush);
                // With a fixed clock, only the flush boundary can dispatch this partial batch.
                assertTrue(partialStarted.await(2, SECONDS));
                assertEquals("partial", partial.get(2, SECONDS));
                assertFalse(flush.isDone());
                var later = batcher.submit("later");
                releaseFirst.countDown();
                flush.get(2, SECONDS);
                assertEquals("first", first.join());
                assertEquals("sibling", sibling.join());
                assertFalse(later.isDone());
                callers.submit(batcher::flush).get(2, SECONDS);
                assertEquals("later", later.join());
            } finally {
                releaseFirst.countDown();
                batcher.close();
            }
        }
    }

    @Test
    void concurrentFlushAndCloseWaitForCancelledDispatchedWorkAndPreserveInterruption()
            throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var batcher = new MicroBatcher<String, String>(config(2, 1, 1), inputs -> {
            started.countDown();
            release.await();
            return List.of(BatchOutcome.success("done"));
        }, () -> 0);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var result = batcher.submit("work");
                var firstFlush = callers.submit(() -> {
                    Thread.currentThread().interrupt();
                    batcher.flush();
                    return Thread.currentThread().isInterrupted();
                });
                assertTrue(started.await(2, SECONDS));
                assertTrue(result.cancel(true));
                var secondFlush = callers.submit(batcher::flush);
                var close = callers.submit(batcher::close);
                assertFalse(firstFlush.isDone());
                assertFalse(secondFlush.isDone());
                assertFalse(close.isDone());
                release.countDown();
                assertTrue(firstFlush.get(2, SECONDS));
                secondFlush.get(2, SECONDS);
                close.get(2, SECONDS);
            } finally {
                release.countDown();
                batcher.close();
            }
        }
    }

    @Test
    void flushWaitsForSynchronousCompletionDelivery() throws Exception {
        var callbackStarted = new CountDownLatch(1);
        var releaseCallback = new Semaphore(0);
        var batcher = new MicroBatcher<String, String>(config(2, 1, 1), inputs ->
                List.of(BatchOutcome.success("done")), () -> 0);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var result = batcher.submit("work");
                var callback = result.thenRun(() -> {
                    callbackStarted.countDown();
                    releaseCallback.acquireUninterruptibly();
                });
                var flush = callers.submit(batcher::flush);
                assertTrue(callbackStarted.await(2, SECONDS));
                assertTrue(result.isDone());
                assertFalse(flush.isDone());
                releaseCallback.release();
                flush.get(2, SECONDS);
                assertTrue(callback.isDone());
            } finally {
                releaseCallback.release();
                batcher.close();
            }
        }
    }

    @Test
    void cancellationIsSkippedAndBackendFailuresRemainOnIndividualFutures() throws Exception {
        var failure = new IOException("backend failed");
        var seen = new LinkedBlockingQueue<List<String>>();
        var batcher = new MicroBatcher<String, String>(config(4, 1, 4), inputs -> {
            seen.add(inputs);
            throw failure;
        }, () -> 0);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                var cancelled = batcher.submit("cancelled");
                assertTrue(cancelled.cancel(false));
                var first = batcher.submit("first");
                var second = batcher.submit("second");
                callers.submit(batcher::flush).get(2, SECONDS);
                assertEquals(List.of(List.of("first", "second")), List.copyOf(seen));
                assertSame(failure, assertThrows(CompletionException.class, first::join).getCause());
                assertSame(failure, assertThrows(CompletionException.class, second::join).getCause());
                assertTrue(cancelled.isCancelled());
            } finally {
                batcher.close();
            }
        }
    }

    private static BatchingConfig config(int batchSize, int concurrentBatches, int pending) {
        return new BatchingConfig(batchSize, Duration.ofHours(1), concurrentBatches, pending,
                AdmissionPolicy.REJECT, Duration.ZERO);
    }
}
