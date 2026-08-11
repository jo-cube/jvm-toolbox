package org.jcube.jvmtoolbox.consumers;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable limits for one {@link LaneConsumer}.
 *
 * @param laneCount ordered logical processing lanes
 * @param maxBatchSize maximum records in one processor invocation
 * @param maxBatchWait age at which the oldest queued record makes its lane eligible
 * @param maxConcurrentBatches maximum processor invocations that may overlap across lanes
 * @param maxInFlightRecords maximum observed records not yet behind a safe commit frontier
 * @param maxInFlightRecordsPerPartition equivalent limit for one assigned partition
 * @param maxPollRecords maximum records accepted from one Kafka poll
 * @param pollTimeout longest Kafka poll wait; shorter batch deadlines take precedence
 */
public record ConsumerProcessingConfig(
        int laneCount,
        int maxBatchSize,
        Duration maxBatchWait,
        int maxConcurrentBatches,
        int maxInFlightRecords,
        int maxInFlightRecordsPerPartition,
        int maxPollRecords,
        Duration pollTimeout) {

    /**
     * Creates validated processing configuration.
     *
     * @throws IllegalArgumentException if a numeric limit is not positive, {@code maxBatchWait} is
     *     negative, {@code pollTimeout} is not positive, or {@code maxPollRecords} exceeds either
     *     in-flight limit
     * @throws NullPointerException if either duration is {@code null}
     * @throws ArithmeticException if either duration cannot be represented in nanoseconds
     */
    public ConsumerProcessingConfig {
        positive(laneCount, "laneCount");
        positive(maxBatchSize, "maxBatchSize");
        positive(maxConcurrentBatches, "maxConcurrentBatches");
        positive(maxInFlightRecords, "maxInFlightRecords");
        positive(maxInFlightRecordsPerPartition, "maxInFlightRecordsPerPartition");
        positive(maxPollRecords, "maxPollRecords");
        Objects.requireNonNull(maxBatchWait, "maxBatchWait");
        Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (maxBatchWait.isNegative()) {
            throw new IllegalArgumentException("maxBatchWait must not be negative");
        }
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be positive");
        }
        if (maxPollRecords > maxInFlightRecords) {
            throw new IllegalArgumentException("maxPollRecords must not exceed maxInFlightRecords");
        }
        if (maxPollRecords > maxInFlightRecordsPerPartition) {
            throw new IllegalArgumentException(
                    "maxPollRecords must not exceed maxInFlightRecordsPerPartition");
        }
        maxBatchWait.toNanos();
        pollTimeout.toNanos();
    }

    private static void positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
