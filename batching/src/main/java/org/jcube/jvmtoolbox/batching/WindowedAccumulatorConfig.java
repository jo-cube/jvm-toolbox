package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.Objects;

/**
 * Immutable limits for a {@link WindowedAccumulator}.
 *
 * @param maxInputsPerWindow maximum successful contributions in one window
 * @param maxWait age at which a non-empty forming window becomes eligible
 * @param maxPendingInputs maximum admitted contributions whose window has not finished processing
 * @param admissionPolicy behavior when {@code maxPendingInputs} is reached
 * @param admissionTimeout timeout used only by {@link AdmissionPolicy#WAIT_WITH_TIMEOUT}; ignored by
 *     the other policies
 */
public record WindowedAccumulatorConfig(
        int maxInputsPerWindow,
        Duration maxWait,
        int maxPendingInputs,
        AdmissionPolicy admissionPolicy,
        Duration admissionTimeout) {

    /**
     * Creates validated accumulation limits.
     *
     * @throws IllegalArgumentException if a limit is not positive, a duration is negative, or timed
     *     admission has no positive timeout
     * @throws NullPointerException if a duration or the admission policy is {@code null}
     * @throws ArithmeticException if either duration cannot be represented in nanoseconds
     */
    public WindowedAccumulatorConfig {
        if (maxInputsPerWindow < 1) {
            throw new IllegalArgumentException("maxInputsPerWindow must be positive");
        }
        if (maxPendingInputs < 1) {
            throw new IllegalArgumentException("maxPendingInputs must be positive");
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
            throw new IllegalArgumentException(
                    "admissionTimeout must be positive for WAIT_WITH_TIMEOUT");
        }
        maxWait.toNanos();
        admissionTimeout.toNanos();
    }
}
