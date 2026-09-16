package org.jcube.jvmtoolbox.batching;

/**
 * Receives opt-in batching lifecycle callbacks.
 *
 * <p>Callbacks are synchronous on the thread causing the event and callbacks for different requests or
 * batches may run concurrently. Per request, admission precedes dispatch, and a batch's dispatch event
 * precedes its terminal event. Cancellation may race with a terminal batch event. Observers should be
 * thread-safe, return promptly, and not call back into the observed batcher. Any exception or error
 * thrown by an observer is ignored so observation cannot change batching results.
 */
public interface BatchObserver {
    /** Why an otherwise valid submission was not admitted. */
    enum RejectionReason {
        /** Pending capacity was exhausted under {@link AdmissionPolicy#REJECT}. */
        CAPACITY,
        /** Waiting admission reached its configured timeout. */
        TIMEOUT,
        /** The batcher was draining or closed. */
        CLOSED
    }

    /** Called once after a submission is admitted and before it can be dispatched. */
    default void onAdmitted() {}

    /**
     * Called before an admission rejection is returned to the caller.
     *
     * @param reason rejection reason
     */
    default void onRejected(RejectionReason reason) {}

    /** Called once when a caller changes an admitted, incomplete completion handle to cancelled. */
    default void onCancelled() {}

    /**
     * Called immediately before a formed batch is handed to backend execution.
     *
     * @param requestCount live caller submissions in the batch at dispatch
     */
    default void onBatchDispatched(int requestCount) {}

    /**
     * Called after a backend returns a valid positional outcome list and those outcomes are applied.
     * Item failures do not make the batch itself failed.
     *
     * @param requestCount caller submissions dispatched in the batch
     * @param successfulRequests futures successfully completed with values
     * @param failedRequests futures successfully completed with item failures; the two completion
     *     counts may total less than {@code requestCount} when callers cancelled after dispatch
     */
    default void onBatchCompleted(int requestCount, int successfulRequests, int failedRequests) {}

    /**
     * Called after a whole-batch failure is applied to its still-live caller futures.
     *
     * @param requestCount caller submissions dispatched in the batch
     * @param failedRequests futures successfully completed with the batch failure; this may be less
     *     than {@code requestCount} when callers cancelled after dispatch
     * @param failure backend, contract, or dispatch failure
     */
    default void onBatchFailed(int requestCount, int failedRequests, Throwable failure) {}

    /**
     * Called by {@link KeyBatchLoader} after equal keys are coalesced and before its backend is invoked.
     *
     * @param requestCount keyed caller submissions in the formed batch
     * @param uniqueKeyCount unique keys handed to the backend
     */
    default void onKeysCoalesced(int requestCount, int uniqueKeyCount) {}

    /**
     * Called once after graceful draining finishes. Later submission attempts may still produce
     * {@link RejectionReason#CLOSED} callbacks.
     */
    default void onClosed() {}
}
