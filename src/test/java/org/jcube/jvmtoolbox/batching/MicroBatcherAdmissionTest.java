package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class MicroBatcherAdmissionTest {
    @Test
    void rejectStopsAtTheDocumentedOutstandingCapacity() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var batcher = blockingBatcher(AdmissionPolicy.REJECT, Duration.ZERO, backendStarted, releaseBackend);
        try {
            var first = batcher.submit("first");
            assertTrue(backendStarted.await(2, SECONDS));

            assertThrows(RejectedExecutionException.class, () -> batcher.submit("second"));
            releaseBackend.countDown();
            assertEquals("first", first.get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
    }

    @Test
    void waitAdmissionResumesAfterOutstandingWorkRetires() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var batcher = blockingBatcher(AdmissionPolicy.WAIT, Duration.ZERO, backendStarted, releaseBackend);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = batcher.submit("first");
            assertTrue(backendStarted.await(2, SECONDS));
            var attemptingAdmission = new CountDownLatch(1);
            var secondAdmission = callers.submit(() -> {
                attemptingAdmission.countDown();
                return batcher.submit("second");
            });

            assertTrue(attemptingAdmission.await(2, SECONDS));
            assertThrows(TimeoutException.class, () -> secondAdmission.get(50, MILLISECONDS));
            releaseBackend.countDown();

            assertEquals("first", first.get(2, SECONDS));
            assertEquals("second", secondAdmission.get(2, SECONDS).get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
    }

    @Test
    void timedWaitFailsWhenCapacityDoesNotBecomeAvailable() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var batcher = blockingBatcher(
                AdmissionPolicy.WAIT_WITH_TIMEOUT,
                Duration.ofMillis(50),
                backendStarted,
                releaseBackend);
        try {
            var first = batcher.submit("first");
            assertTrue(backendStarted.await(2, SECONDS));

            assertThrows(TimeoutException.class, () -> batcher.submit("second"));
            releaseBackend.countDown();
            assertEquals("first", first.get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
    }

    @Test
    void closingWakesWaitingAdmissionAndRejectsNewWork() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var batcher = blockingBatcher(AdmissionPolicy.WAIT, Duration.ZERO, backendStarted, releaseBackend);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = batcher.submit("first");
            assertTrue(backendStarted.await(2, SECONDS));
            var attemptingAdmission = new CountDownLatch(1);
            var waitingAdmission = callers.submit(() -> {
                attemptingAdmission.countDown();
                return batcher.submit("waiting");
            });
            assertTrue(attemptingAdmission.await(2, SECONDS));
            assertThrows(TimeoutException.class, () -> waitingAdmission.get(50, MILLISECONDS));

            var close = callers.submit(batcher::close);
            var rejection = assertThrows(ExecutionException.class, () -> waitingAdmission.get(2, SECONDS));
            assertInstanceOf(RejectedExecutionException.class, rejection.getCause());
            assertThrows(RejectedExecutionException.class, () -> batcher.submit("late"));

            releaseBackend.countDown();
            assertEquals("first", first.get(2, SECONDS));
            close.get(2, SECONDS);
        } finally {
            releaseBackend.countDown();
            batcher.close();
        }
    }

    private static MicroBatcher<String, String> blockingBatcher(
            AdmissionPolicy policy,
            Duration admissionTimeout,
            CountDownLatch backendStarted,
            CountDownLatch releaseBackend) {
        var config = new BatchingConfig(
                1,
                Duration.ZERO,
                1,
                1,
                policy,
                admissionTimeout);
        return new MicroBatcher<>(config, inputs -> {
            backendStarted.countDown();
            releaseBackend.await();
            return List.of(BatchOutcome.success(inputs.getFirst()));
        });
    }
}
