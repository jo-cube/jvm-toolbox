package org.jcube.jvmtoolbox.batching;

/** Controls synchronous admission when the configured pending-request capacity is exhausted. */
public enum AdmissionPolicy {
    /** Reject immediately with {@link java.util.concurrent.RejectedExecutionException}. */
    REJECT,
    /** Block the submitting thread until capacity becomes available or the batcher starts closing. */
    WAIT,
    /** Block up to {@link BatchingConfig#admissionTimeout()}, then throw {@link java.util.concurrent.TimeoutException}. */
    WAIT_WITH_TIMEOUT
}
