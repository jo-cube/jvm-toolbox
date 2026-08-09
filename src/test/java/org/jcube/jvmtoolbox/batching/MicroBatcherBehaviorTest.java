package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

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
        try (var batcher = new MicroBatcher<String, String>(
                config(1), inputs -> Collections.singletonList(null))) {
            var failure = assertThrows(
                            ExecutionException.class,
                            () -> batcher.submit("input").get(2, SECONDS))
                    .getCause();

            assertEquals(IllegalStateException.class, failure.getClass());
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
