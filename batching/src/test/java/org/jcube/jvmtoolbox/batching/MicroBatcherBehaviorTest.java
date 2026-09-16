package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MicroBatcherBehaviorTest {
    @Test
    void processorReceivesAnUnmodifiableBatch() throws Exception {
        try (var batcher = new MicroBatcher<String, String>(config(1), inputs -> {
            assertThrows(UnsupportedOperationException.class, () -> inputs.add("other"));
            return List.of(BatchOutcome.success(inputs.getFirst()));
        })) {
            assertEquals("input", batcher.submit("input").get(2, SECONDS));
        }
    }

    @Test
    void successfulOutcomeMayContainNull() throws Exception {
        try (var batcher = new MicroBatcher<String, Void>(
                config(1), inputs -> List.of(BatchOutcome.success(null)))) {
            assertNull(batcher.submit("input").get(2, SECONDS));
        }
    }

    @Test
    void preservesPositionAndTreatsDuplicateInputsIndependently() throws Exception {
        try (var batcher = new MicroBatcher<String, String>(config(2), inputs -> List.of(
                BatchOutcome.success(inputs.get(0) + "-first"),
                BatchOutcome.success(inputs.get(1) + "-second")))) {
            var first = batcher.submit("same");
            var second = batcher.submit("same");

            assertEquals("same-first", first.get(2, SECONDS));
            assertEquals("same-second", second.get(2, SECONDS));
        }
    }

    @Test
    void keepsItemFailureLocalToItsPosition() throws Exception {
        var failure = new IllegalArgumentException("bad item");
        try (var batcher = new MicroBatcher<String, String>(config(2), inputs -> List.of(
                BatchOutcome.success("ok"), BatchOutcome.failure(failure)))) {
            var first = batcher.submit("good");
            var second = batcher.submit("bad");

            assertEquals("ok", first.get(2, SECONDS));
            assertSame(failure, assertThrows(ExecutionException.class, () -> second.get(2, SECONDS)).getCause());
        }
    }

    @Test
    void failsEveryPositionWhenTheBatchProcessorThrows() throws Exception {
        var failure = new IllegalStateException("backend failed");
        try (var batcher = new MicroBatcher<String, String>(config(2), inputs -> {
            throw failure;
        })) {
            var first = batcher.submit("a");
            var second = batcher.submit("b");

            assertSame(failure, assertThrows(ExecutionException.class, () -> first.get(2, SECONDS)).getCause());
            assertSame(failure, assertThrows(ExecutionException.class, () -> second.get(2, SECONDS)).getCause());
        }
    }

    @Test
    void failsTheWholeBatchForTheWrongNumberOfOutcomes() throws Exception {
        try (var batcher = new MicroBatcher<String, String>(config(2), inputs -> List.of())) {
            var first = batcher.submit("a");
            var second = batcher.submit("b");

            var firstFailure = assertThrows(ExecutionException.class, () -> first.get(2, SECONDS)).getCause();
            var secondFailure = assertThrows(ExecutionException.class, () -> second.get(2, SECONDS)).getCause();
            assertEquals(IllegalStateException.class, firstFailure.getClass());
            assertSame(firstFailure, secondFailure);
        }
    }

    @Test
    void nullOutcomeListFailsTheWholeBatch() throws Exception {
        try (var batcher = new MicroBatcher<String, String>(config(1), inputs -> null)) {
            var failure = assertThrows(
                            ExecutionException.class,
                            () -> batcher.submit("input").get(2, SECONDS))
                    .getCause();

            assertEquals(IllegalStateException.class, failure.getClass());
        }
    }

    @Test
    void nullOutcomeFailsTheWholeBatch() throws Exception {
        try (var batcher = new MicroBatcher<String, String>(config(2), inputs ->
                new LinkedList<>(Arrays.asList(BatchOutcome.success("must not escape"), null)))) {
            var first = batcher.submit("first");
            var second = batcher.submit("second");
            var failure = assertThrows(ExecutionException.class, () -> first.get(2, SECONDS)).getCause();

            assertEquals(IllegalStateException.class, failure.getClass());
            assertSame(failure, assertThrows(ExecutionException.class, () -> second.get(2, SECONDS)).getCause());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sequentialOutcomesPreservePositionsAndFailures(boolean observed) throws Exception {
        var failure = new IllegalArgumentException("bad item");
        BatchProcessor<String, String> processor = inputs -> new LinkedList<>(List.of(
                BatchOutcome.success(inputs.getFirst()),
                BatchOutcome.failure(failure),
                BatchOutcome.success(null),
                BatchOutcome.success(inputs.getLast())));
        var statistics = new BatchStatistics();
        try (var batcher = observed
                ? new MicroBatcher<>(config(4), processor, statistics)
                : new MicroBatcher<>(config(4), processor)) {
            var first = batcher.submit("first");
            var second = batcher.submit("second");
            var third = batcher.submit("third");
            var fourth = batcher.submit("fourth");
            batcher.flush();

            assertEquals("first", first.get(2, SECONDS));
            assertSame(failure, assertThrows(ExecutionException.class, second::get).getCause());
            assertNull(third.get(2, SECONDS));
            assertEquals("fourth", fourth.get(2, SECONDS));
            if (observed) {
                assertEquals(3, statistics.snapshot().successfulRequests());
                assertEquals(1, statistics.snapshot().failedRequests());
            }
        }
    }

    private static BatchingConfig config(int maxBatchSize) {
        return new BatchingConfig(
                maxBatchSize,
                Duration.ofSeconds(1),
                1,
                8,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
    }
}
