package org.jcube.jvmtoolbox.batching;

/** Controls synchronous admission when a configured pending-work capacity is exhausted. */
public enum AdmissionPolicy {
    /** Reject immediately with {@link java.util.concurrent.RejectedExecutionException}. */
    REJECT,
    /** Block the submitting thread until capacity becomes available or the component starts closing. */
    WAIT,
    /** Block up to the configured admission timeout, then throw {@link java.util.concurrent.TimeoutException}. */
    WAIT_WITH_TIMEOUT
}
