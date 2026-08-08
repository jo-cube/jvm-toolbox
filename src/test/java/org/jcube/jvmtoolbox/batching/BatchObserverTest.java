package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class BatchObserverTest {
    @Test
    void successfulBatchEventsFollowAdmissionAndDispatch() throws Exception {
        var itemFailure = new IllegalArgumentException("item failed");
        var observer = new RecordingObserver();
        var batcher = new MicroBatcher<String, String>(config(2, 2), inputs -> List.of(
                BatchOutcome.success("value"), BatchOutcome.failure(itemFailure)), observer);
        try {
            var successful = batcher.submit("good");
            var failed = batcher.submit("bad");

            assertEquals("value", successful.get(2, SECONDS));
            assertSame(itemFailure, assertThrows(ExecutionException.class, () -> failed.get(2, SECONDS)).getCause());
        } finally {
            batcher.close();
        }

        assertEquals(
                List.of("admitted", "admitted", "dispatched:2", "completed:2:1:1", "closed"),
                observer.events);
    }

    @Test
    void rejectionIsObservedBeforeItReturnsToTheCaller() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var observer = new RecordingObserver();
        var batcher = new MicroBatcher<String, String>(config(1, 1), inputs -> {
            backendStarted.countDown();
            releaseBackend.await();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        }, observer);
        try {
            var accepted = batcher.submit("accepted");
            assertTrue(backendStarted.await(2, SECONDS));

            assertThrows(RejectedExecutionException.class, () -> batcher.submit("rejected"));
            releaseBackend.countDown();
            assertEquals("accepted", accepted.get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }

        assertEquals(
                List.of("admitted", "dispatched:1", "rejected:CAPACITY", "completed:1:1:0", "closed"),
                observer.events);
    }

    @Test
    void timeoutAndClosedRejectionsHaveDistinctReasons() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var observer = new RecordingObserver();
        var config = new BatchingConfig(
                1,
                Duration.ZERO,
                1,
                1,
                AdmissionPolicy.WAIT_WITH_TIMEOUT,
                Duration.ofMillis(20));
        var batcher = new MicroBatcher<String, String>(config, inputs -> {
            backendStarted.countDown();
            releaseBackend.await();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        }, observer);
        try {
            var accepted = batcher.submit("accepted");
            assertTrue(backendStarted.await(2, SECONDS));
            assertThrows(TimeoutException.class, () -> batcher.submit("timed-out"));

            releaseBackend.countDown();
            assertEquals("accepted", accepted.get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
        assertThrows(RejectedExecutionException.class, () -> batcher.submit("closed"));

        assertEquals(
                List.of(
                        "admitted",
                        "dispatched:1",
                        "rejected:TIMEOUT",
                        "completed:1:1:0",
                        "closed",
                        "rejected:CLOSED"),
                observer.events);
    }

    @Test
    void successfulCancellationIsObservedWithoutChangingSiblingCompletion() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var observer = new RecordingObserver();
        var batcher = new MicroBatcher<String, String>(config(2, 2), inputs -> {
            backendStarted.countDown();
            releaseBackend.await();
            return inputs.stream().map(BatchOutcome::success).toList();
        }, observer);
        try {
            var cancelled = batcher.submit("cancelled");
            var live = batcher.submit("live");
            assertTrue(backendStarted.await(2, SECONDS));

            cancelled.cancel(false);
            releaseBackend.countDown();

            assertThrows(CancellationException.class, () -> cancelled.get(2, SECONDS));
            assertEquals("live", live.get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }

        assertEquals(
                List.of("admitted", "admitted", "dispatched:2", "cancelled", "completed:2:1:0", "closed"),
                observer.events);
    }

    @Test
    void wholeBatchFailureHasItsOwnTerminalEvent() throws Exception {
        var backendFailure = new IllegalStateException("backend failed");
        var observer = new RecordingObserver();
        var batcher = new MicroBatcher<String, String>(config(2, 2), inputs -> {
            throw backendFailure;
        }, observer);
        try {
            var first = batcher.submit("first");
            var second = batcher.submit("second");

            assertSame(backendFailure, assertThrows(ExecutionException.class, () -> first.get(2, SECONDS)).getCause());
            assertSame(backendFailure, assertThrows(ExecutionException.class, () -> second.get(2, SECONDS)).getCause());
        } finally {
            batcher.close();
        }

        assertEquals(
                List.of("admitted", "admitted", "dispatched:2", "failed:2:2", "closed"),
                observer.events);
    }

    @Test
    void keyedCoalescingUsesTheSameObserverSequence() throws Exception {
        var observer = new RecordingObserver();
        var loader = new KeyBatchLoader<String, String>(config(3, 3), keys -> Map.of(
                "same", BatchOutcome.success("shared"),
                "other", BatchOutcome.success("different")), observer);
        try {
            var first = loader.load(new String("same"));
            var duplicate = loader.load(new String("same"));
            var other = loader.load("other");

            assertEquals(Optional.of("shared"), first.get(2, SECONDS));
            assertEquals(Optional.of("shared"), duplicate.get(2, SECONDS));
            assertEquals(Optional.of("different"), other.get(2, SECONDS));
        } finally {
            loader.close();
        }

        assertEquals(
                List.of(
                        "admitted",
                        "admitted",
                        "admitted",
                        "dispatched:3",
                        "keys:3:2",
                        "completed:3:3:0",
                        "closed"),
                observer.events);
    }

    @Test
    void observerFailuresNeverReplaceBatchingResultsOrRejections() throws Exception {
        var backendFailure = new IllegalStateException("backend failed");
        BatchObserver observer = new ThrowingObserver();
        var batcher = new MicroBatcher<String, String>(config(1, 1), inputs -> {
            throw backendFailure;
        }, observer);
        try {
            var result = batcher.submit("input");
            assertSame(backendFailure, assertThrows(ExecutionException.class, () -> result.get(2, SECONDS)).getCause());
        } finally {
            batcher.close();
        }

        assertThrows(RejectedExecutionException.class, () -> batcher.submit("late"));
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

    private static final class RecordingObserver implements BatchObserver {
        private final List<String> events = new CopyOnWriteArrayList<>();

        @Override
        public void onAdmitted() {
            events.add("admitted");
        }

        @Override
        public void onRejected(RejectionReason reason) {
            events.add("rejected:" + reason);
        }

        @Override
        public void onCancelled() {
            events.add("cancelled");
        }

        @Override
        public void onBatchDispatched(int requestCount) {
            events.add("dispatched:" + requestCount);
        }

        @Override
        public void onBatchCompleted(int requestCount, int successfulRequests, int failedRequests) {
            events.add("completed:" + requestCount + ":" + successfulRequests + ":" + failedRequests);
        }

        @Override
        public void onBatchFailed(int requestCount, int failedRequests, Throwable failure) {
            events.add("failed:" + requestCount + ":" + failedRequests);
        }

        @Override
        public void onKeysCoalesced(int requestCount, int uniqueKeyCount) {
            events.add("keys:" + requestCount + ":" + uniqueKeyCount);
        }

        @Override
        public void onClosed() {
            events.add("closed");
        }
    }

    private static final class ThrowingObserver implements BatchObserver {
        private static AssertionError failure() {
            return new AssertionError("observer failed");
        }

        @Override
        public void onAdmitted() {
            throw failure();
        }

        @Override
        public void onRejected(RejectionReason reason) {
            throw failure();
        }

        @Override
        public void onBatchDispatched(int requestCount) {
            throw failure();
        }

        @Override
        public void onBatchFailed(int requestCount, int failedRequests, Throwable failure) {
            throw failure();
        }

        @Override
        public void onClosed() {
            throw failure();
        }
    }
}
