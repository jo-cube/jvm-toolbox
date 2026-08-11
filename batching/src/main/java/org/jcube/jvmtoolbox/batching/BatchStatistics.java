package org.jcube.jvmtoolbox.batching;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe observer that accumulates simple counters and current gauges.
 *
 * <p>Snapshots are immutable and weakly consistent: concurrent callback updates may become visible
 * across fields at slightly different times.
 * Counters are cumulative for the lifetime of this collector and are not reset by {@link #snapshot()}.
 * The incomplete-request gauge follows terminal observer callbacks, not admission-capacity
 * bookkeeping, so replacement admissions may make it temporarily exceed a batcher's pending limit.
 */
public final class BatchStatistics implements BatchObserver {
    private final LongAdder admittedRequests = new LongAdder();
    private final LongAdder rejectedRequests = new LongAdder();
    private final LongAdder cancelledRequests = new LongAdder();
    private final LongAdder dispatchedBatches = new LongAdder();
    private final LongAdder completedBatches = new LongAdder();
    private final LongAdder failedBatches = new LongAdder();
    private final LongAdder dispatchedRequests = new LongAdder();
    private final LongAdder successfulRequests = new LongAdder();
    private final LongAdder failedRequests = new LongAdder();
    private final LongAdder keyedRequests = new LongAdder();
    private final LongAdder uniqueKeys = new LongAdder();
    private final LongAdder coalescedRequests = new LongAdder();
    private final AtomicInteger incompleteRequests = new AtomicInteger();
    private final AtomicInteger batchesInFlight = new AtomicInteger();
    private volatile boolean closed;

    /** Creates an empty statistics collector. */
    public BatchStatistics() {}

    @Override
    public void onAdmitted() {
        admittedRequests.increment();
        incompleteRequests.incrementAndGet();
    }

    @Override
    public void onRejected(RejectionReason reason) {
        rejectedRequests.increment();
    }

    @Override
    public void onCancelled() {
        cancelledRequests.increment();
        incompleteRequests.decrementAndGet();
    }

    @Override
    public void onBatchDispatched(int requestCount) {
        dispatchedBatches.increment();
        dispatchedRequests.add(requestCount);
        batchesInFlight.incrementAndGet();
    }

    @Override
    public void onBatchCompleted(int requestCount, int successful, int failed) {
        completedBatches.increment();
        successfulRequests.add(successful);
        failedRequests.add(failed);
        incompleteRequests.addAndGet(-successful - failed);
        batchesInFlight.decrementAndGet();
    }

    @Override
    public void onBatchFailed(int requestCount, int failed, Throwable failure) {
        failedBatches.increment();
        failedRequests.add(failed);
        incompleteRequests.addAndGet(-failed);
        batchesInFlight.decrementAndGet();
    }

    @Override
    public void onKeysCoalesced(int requestCount, int uniqueKeyCount) {
        keyedRequests.add(requestCount);
        uniqueKeys.add(uniqueKeyCount);
        coalescedRequests.add(requestCount - uniqueKeyCount);
    }

    @Override
    public void onClosed() {
        closed = true;
    }

    /**
     * Takes a weakly consistent snapshot.
     *
     * @return an immutable view of the currently observed values
     */
    public Snapshot snapshot() {
        return new Snapshot(
                admittedRequests.sum(),
                rejectedRequests.sum(),
                cancelledRequests.sum(),
                dispatchedBatches.sum(),
                completedBatches.sum(),
                failedBatches.sum(),
                dispatchedRequests.sum(),
                successfulRequests.sum(),
                failedRequests.sum(),
                keyedRequests.sum(),
                uniqueKeys.sum(),
                coalescedRequests.sum(),
                incompleteRequests.get(),
                batchesInFlight.get(),
                closed);
    }

    /**
     * Immutable counter and gauge snapshot.
     *
     * @param admittedRequests successfully admitted caller submissions
     * @param rejectedRequests submissions rejected for capacity, timeout, or lifecycle state
     * @param cancelledRequests admitted futures successfully cancelled by callers
     * @param dispatchedBatches batches handed to backend execution
     * @param completedBatches batches whose backend returned a valid outcome list
     * @param failedBatches batches that failed during dispatch, backend execution, or result validation
     * @param dispatchedRequests caller submissions present at batch dispatch
     * @param successfulRequests futures completed with successful values, including missing keyed values
     * @param failedRequests futures completed exceptionally by item or whole-batch failures
     * @param keyedRequests keyed caller submissions considered for coalescing
     * @param uniqueKeys unique keys handed to keyed backends
     * @param coalescedRequests keyed submissions removed from backend work as duplicates
     * @param incompleteRequests admitted futures not yet reflected by a terminal observer callback or
     *     cancellation
     * @param batchesInFlight dispatched batches without a completion or failure callback
     * @param closed whether graceful draining has finished
     */
    public record Snapshot(
            long admittedRequests,
            long rejectedRequests,
            long cancelledRequests,
            long dispatchedBatches,
            long completedBatches,
            long failedBatches,
            long dispatchedRequests,
            long successfulRequests,
            long failedRequests,
            long keyedRequests,
            long uniqueKeys,
            long coalescedRequests,
            int incompleteRequests,
            int batchesInFlight,
            boolean closed) {}
}
