package org.jcube.jvmtoolbox.batching;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BatchingConfigTest {
    @Test
    void rejectsInvalidLimitsAndDurations() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> config(0, 1, 1)),
                () -> assertThrows(IllegalArgumentException.class, () -> config(1, 0, 1)),
                () -> assertThrows(IllegalArgumentException.class, () -> config(1, 1, 0)),
                () -> assertThrows(IllegalArgumentException.class, () -> new BatchingConfig(
                        1,
                        Duration.ofNanos(-1),
                        1,
                        1,
                        AdmissionPolicy.REJECT,
                        Duration.ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> new BatchingConfig(
                        1,
                        Duration.ZERO,
                        1,
                        1,
                        AdmissionPolicy.WAIT_WITH_TIMEOUT,
                        Duration.ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> new BatchingConfig(
                        1,
                        Duration.ZERO,
                        1,
                        1,
                        AdmissionPolicy.WAIT,
                        Duration.ofNanos(-1))),
                () -> assertThrows(ArithmeticException.class, () -> new BatchingConfig(
                        1,
                        Duration.ofSeconds(Long.MAX_VALUE),
                        1,
                        1,
                        AdmissionPolicy.REJECT,
                        Duration.ZERO)));
    }

    @Test
    void rejectsNullContractValues() {
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new BatchingConfig(
                        1, null, 1, 1, AdmissionPolicy.REJECT, Duration.ZERO)),
                () -> assertThrows(NullPointerException.class, () -> new BatchingConfig(
                        1, Duration.ZERO, 1, 1, null, Duration.ZERO)),
                () -> assertThrows(NullPointerException.class, () -> new BatchingConfig(
                        1, Duration.ZERO, 1, 1, AdmissionPolicy.REJECT, null)));
    }

    private static BatchingConfig config(
            int maxBatchSize, int maxConcurrentBatches, int maxPendingRequests) {
        return new BatchingConfig(
                maxBatchSize,
                Duration.ZERO,
                maxConcurrentBatches,
                maxPendingRequests,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
    }
}
