package org.jcube.jvmtoolbox.batching;

final class BatchObserverSupport {
    private BatchObserverSupport() {}

    static void admitted(BatchObserver observer) {
        try {
            observer.onAdmitted();
        } catch (Throwable ignored) {
        }
    }

    static void rejected(BatchObserver observer, BatchObserver.RejectionReason reason) {
        try {
            observer.onRejected(reason);
        } catch (Throwable ignored) {
        }
    }

    static void cancelled(BatchObserver observer) {
        try {
            observer.onCancelled();
        } catch (Throwable ignored) {
        }
    }

    static void batchDispatched(BatchObserver observer, int requestCount) {
        try {
            observer.onBatchDispatched(requestCount);
        } catch (Throwable ignored) {
        }
    }

    static void batchCompleted(
            BatchObserver observer, int requestCount, int successfulRequests, int failedRequests) {
        try {
            observer.onBatchCompleted(requestCount, successfulRequests, failedRequests);
        } catch (Throwable ignored) {
        }
    }

    static void batchFailed(
            BatchObserver observer, int requestCount, int failedRequests, Throwable failure) {
        try {
            observer.onBatchFailed(requestCount, failedRequests, failure);
        } catch (Throwable ignored) {
        }
    }

    static void keysCoalesced(BatchObserver observer, int requestCount, int uniqueKeyCount) {
        try {
            observer.onKeysCoalesced(requestCount, uniqueKeyCount);
        } catch (Throwable ignored) {
        }
    }

    static void closed(BatchObserver observer) {
        try {
            observer.onClosed();
        } catch (Throwable ignored) {
        }
    }
}
