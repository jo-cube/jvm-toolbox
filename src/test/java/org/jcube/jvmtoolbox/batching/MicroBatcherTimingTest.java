package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MicroBatcherTimingTest {
    private static final long MAX_WAIT = Duration.ofHours(1).toNanos();

    @Test
    void oneSubmissionFormsWhenItsMaxWaitExpires() throws Exception {
        var clock = new AtomicLong();
        var batches = new LinkedBlockingQueue<List<String>>();
        try (var batcher = batcher(3, clock, batches)) {
            var result = batcher.submit("only");

            assertNull(batches.poll(50, MILLISECONDS));
            clock.set(MAX_WAIT);
            batcher.signalCoordinator();

            assertEquals(List.of("only"), batches.poll(2, SECONDS));
            assertEquals("only", result.get(2, SECONDS));
        }
    }

    @Test
    void maxBatchSizeDispatchesWithoutClockAdvancing() throws Exception {
        var clock = new AtomicLong();
        var batches = new LinkedBlockingQueue<List<String>>();
        try (var batcher = batcher(3, clock, batches)) {
            var first = batcher.submit("a");
            var second = batcher.submit("b");
            var third = batcher.submit("c");

            assertEquals(List.of("a", "b", "c"), batches.poll(2, SECONDS));
            assertEquals("a", first.get(2, SECONDS));
            assertEquals("b", second.get(2, SECONDS));
            assertEquals("c", third.get(2, SECONDS));
        }
    }

    @Test
    void oldestSubmissionControlsTheDeadline() throws Exception {
        var clock = new AtomicLong();
        var batches = new LinkedBlockingQueue<List<String>>();
        try (var batcher = batcher(3, clock, batches)) {
            var first = batcher.submit("oldest");
            clock.set(Duration.ofMinutes(30).toNanos());
            var second = batcher.submit("newer");

            clock.set(Duration.ofMinutes(59).toNanos());
            batcher.signalCoordinator();
            assertNull(batches.poll(50, MILLISECONDS));

            clock.set(MAX_WAIT);
            batcher.signalCoordinator();
            assertEquals(List.of("oldest", "newer"), batches.poll(2, SECONDS));
            assertEquals("oldest", first.get(2, SECONDS));
            assertEquals("newer", second.get(2, SECONDS));
        }
    }

    @Test
    void realClockWakesTheCoordinatorAtTheDeadline() throws Exception {
        var config = new BatchingConfig(
                2,
                Duration.ofMillis(20),
                1,
                2,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
        try (var batcher = new MicroBatcher<String, String>(config, inputs ->
                List.of(BatchOutcome.success(inputs.getFirst())))) {
            assertEquals("only", batcher.submit("only").get(2, SECONDS));
        }
    }

    private static MicroBatcher<String, String> batcher(
            int maxBatchSize, AtomicLong clock, LinkedBlockingQueue<List<String>> batches) {
        var config = new BatchingConfig(
                maxBatchSize,
                Duration.ofHours(1),
                1,
                8,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
        return new MicroBatcher<>(config, inputs -> {
            batches.add(inputs);
            return inputs.stream().map(BatchOutcome::success).toList();
        }, clock::get);
    }
}
