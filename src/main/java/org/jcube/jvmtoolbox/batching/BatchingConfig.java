package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable configuration shared by all submissions to a {@link MicroBatcher} or {@link
 * KeyBatchLoader}.
 *
 * @param maxBatchSize maximum inputs in one backend invocation
 * @param maxWait age at which the oldest forming submission makes its batch eligible
 * @param maxConcurrentBatches maximum backend invocations that may overlap
 * @param maxPendingRequests maximum admitted submissions whose batch has not finished; a cancelled
 *     submission keeps its slot until the coordinator observes it or its dispatched batch returns
 * @param admissionPolicy behavior when {@code maxPendingRequests} is reached
 * @param admissionTimeout timeout used only by {@link AdmissionPolicy#WAIT_WITH_TIMEOUT}; ignored by
 *     the other policies
 */
public record BatchingConfig(
        int maxBatchSize,
        Duration maxWait,
        int maxConcurrentBatches,
        int maxPendingRequests,
        AdmissionPolicy admissionPolicy,
        Duration admissionTimeout) {

    /**
     * Creates validated batching configuration.
     *
     * @throws IllegalArgumentException if a size or concurrency limit is not positive, a duration is
     *     negative, or timed admission has no positive timeout
     * @throws NullPointerException if a duration or the admission policy is {@code null}
     * @throws ArithmeticException if either duration cannot be represented in nanoseconds
     */
    public BatchingConfig {
        if (maxBatchSize < 1) {
            throw new IllegalArgumentException("maxBatchSize must be positive");
        }
        if (maxConcurrentBatches < 1) {
            throw new IllegalArgumentException("maxConcurrentBatches must be positive");
        }
        if (maxPendingRequests < 1) {
            throw new IllegalArgumentException("maxPendingRequests must be positive");
        }
        Objects.requireNonNull(maxWait, "maxWait");
        Objects.requireNonNull(admissionPolicy, "admissionPolicy");
        Objects.requireNonNull(admissionTimeout, "admissionTimeout");
        if (maxWait.isNegative()) {
            throw new IllegalArgumentException("maxWait must not be negative");
        }
        if (admissionTimeout.isNegative()) {
            throw new IllegalArgumentException("admissionTimeout must not be negative");
        }
        if (admissionPolicy == AdmissionPolicy.WAIT_WITH_TIMEOUT && admissionTimeout.isZero()) {
            throw new IllegalArgumentException("admissionTimeout must be positive for WAIT_WITH_TIMEOUT");
        }
        maxWait.toNanos();
        admissionTimeout.toNanos();
    }
}
