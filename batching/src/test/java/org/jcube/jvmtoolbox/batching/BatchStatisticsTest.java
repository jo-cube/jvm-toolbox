package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import org.junit.jupiter.api.Test;

class BatchStatisticsTest {
    @Test
    void snapshotCountsAdmissionCancellationRejectionAndKeyCoalescing() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var statistics = new BatchStatistics();
        var loader = new KeyBatchLoader<String, String>(config(2, 2), keys -> {
            backendStarted.countDown();
            releaseBackend.await();
            return Map.of("same", BatchOutcome.success("value"));
        }, statistics);
        try {
            var cancelled = loader.load(new String("same"));
            var live = loader.load(new String("same"));
            assertTrue(backendStarted.await(2, SECONDS));
            assertEquals(2, statistics.snapshot().incompleteRequests());
            assertEquals(1, statistics.snapshot().batchesInFlight());

            try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
                var cancellations = new ArrayList<java.util.concurrent.Future<Boolean>>();
                var start = new CountDownLatch(1);
                for (int index = 0; index < 16; index++) {
                    cancellations.add(callers.submit(() -> {
                        start.await();
                        return cancelled.cancel(false);
                    }));
                }
                start.countDown();
                for (var cancellation : cancellations) {
                    assertTrue(cancellation.get(2, SECONDS));
                }
            }
            assertTrue(cancelled.cancel(true));
            assertEquals(1, statistics.snapshot().incompleteRequests());
            assertThrows(RejectedExecutionException.class, () -> loader.load("rejected"));
            releaseBackend.countDown();
            assertEquals(Optional.of("value"), live.get(2, SECONDS));
            assertFalse(live.cancel(false));
        } finally {
            releaseBackend.countDown();
            loader.close();
        }

        assertEquals(
                new BatchStatistics.Snapshot(2, 1, 1, 1, 1, 0, 2, 1, 0, 2, 1, 1, 0, 0, true),
                statistics.snapshot());
    }

    @Test
    void snapshotSeparatesWholeBatchFailuresFromNormalCompletion() throws Exception {
        var statistics = new BatchStatistics();
        var failure = new IllegalStateException("backend failed");
        var batcher = new MicroBatcher<String, String>(config(2, 2), inputs -> {
            throw failure;
        }, statistics);
        try {
            var first = batcher.submit("first");
            var second = batcher.submit("second");
            assertThrows(ExecutionException.class, () -> first.get(2, SECONDS));
            assertThrows(ExecutionException.class, () -> second.get(2, SECONDS));
        } finally {
            batcher.close();
        }

        assertEquals(
                new BatchStatistics.Snapshot(2, 0, 0, 1, 0, 1, 2, 0, 2, 0, 0, 0, 0, 0, true),
                statistics.snapshot());
    }

    private static BatchingConfig config(int maxBatchSize, int capacity) {
        return new BatchingConfig(
                maxBatchSize,
                Duration.ofSeconds(1),
                1,
                capacity,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
    }
}
