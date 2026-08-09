package org.jcube.jvmtoolbox.perf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.jcube.jvmtoolbox.batching.BatchOutcome;

final class SyntheticBackend {
    static final RuntimeException FAILURE = new SyntheticFailure();

    private final PerfScenario scenario;
    private final LoadMetrics metrics;
    private final AtomicLong batches = new AtomicLong();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maximumActive = new AtomicInteger();
    private final AtomicInteger concurrencyViolations = new AtomicInteger();

    SyntheticBackend(PerfScenario scenario, LoadMetrics metrics) {
        this.scenario = scenario;
        this.metrics = metrics;
    }

    List<BatchOutcome<Long>> process(List<Request> requests) {
        long started = System.nanoTime();
        for (Request request : requests) {
            metrics.recordQueueDelay(started - request.submitNanos());
        }
        begin();
        long batchNumber = batches.incrementAndGet();
        try {
            delay(batchNumber);
            failBatch(batchNumber);
            var outcomes = new ArrayList<BatchOutcome<Long>>(requests.size());
            for (Request request : requests) {
                outcomes.add(fails(request.id())
                        ? BatchOutcome.failure(FAILURE)
                        : BatchOutcome.success(request.id()));
            }
            return outcomes;
        } finally {
            end(started);
        }
    }

    Map<KeyRequest, BatchOutcome<Integer>> load(Set<KeyRequest> keys) {
        long started = System.nanoTime();
        for (KeyRequest key : keys) {
            metrics.recordQueueDelay(started - key.submitNanos);
        }
        begin();
        long batchNumber = batches.incrementAndGet();
        try {
            delay(batchNumber);
            failBatch(batchNumber);
            var results = new HashMap<KeyRequest, BatchOutcome<Integer>>(keys.size());
            for (KeyRequest key : keys) {
                if (missing(key.key)) {
                    continue;
                }
                results.put(
                        key,
                        fails(key.key)
                                ? BatchOutcome.failure(FAILURE)
                                : BatchOutcome.success(key.key));
            }
            return results;
        } finally {
            end(started);
        }
    }

    int maximumActive() {
        return maximumActive.get();
    }

    int concurrencyViolations() {
        return concurrencyViolations.get();
    }

    boolean expected(Throwable failure) {
        return failure == FAILURE;
    }

    private void begin() {
        int current = active.incrementAndGet();
        maximumActive.accumulateAndGet(current, Math::max);
        if (current > scenario.maxConcurrentBatches()) {
            concurrencyViolations.incrementAndGet();
        }
    }

    private void end(long started) {
        metrics.recordBackendLatency(System.nanoTime() - started);
        active.decrementAndGet();
    }

    private void delay(long batchNumber) {
        BackendModel model = scenario.backend();
        long jitter = model.jitter().toNanos();
        long offset = jitter == 0
                ? 0
                : Math.floorMod(mix(batchNumber), jitter * 2 + 1) - jitter;
        long delay = model.latency().toNanos() + offset;
        long elapsed = metrics.elapsedNanos(System.nanoTime());
        long slowdownEnd = model.slowdownStart().plus(model.slowdownDuration()).toNanos();
        if (!model.slowdownDuration().isZero()
                && elapsed >= model.slowdownStart().toNanos()
                && elapsed < slowdownEnd) {
            delay = Math.round(delay * model.slowdownFactor());
        }
        long deadline = System.nanoTime() + delay;
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(remaining);
        }
    }

    private void failBatch(long batchNumber) {
        int every = scenario.backend().batchFailureEvery();
        if (every > 0 && batchNumber % every == 0) {
            throw FAILURE;
        }
    }

    private boolean missing(long value) {
        int every = scenario.backend().missingEvery();
        return every > 0 && Math.floorMod(value, every) == 0;
    }

    private boolean fails(long value) {
        int every = scenario.backend().itemFailureEvery();
        return every > 0 && Math.floorMod(value, every) == 1;
    }

    private static long mix(long value) {
        value = (value ^ (value >>> 33)) * 0xff51afd7ed558ccdl;
        value = (value ^ (value >>> 33)) * 0xc4ceb9fe1a85ec53l;
        return value ^ (value >>> 33);
    }

    private static final class SyntheticFailure extends RuntimeException {
        private SyntheticFailure() {
            super("synthetic backend failure", null, false, false);
        }
    }
}

record Request(long id, long scheduledNanos, long submitNanos) {}

final class KeyRequest {
    final long id;
    final int key;
    final long scheduledNanos;
    final long submitNanos;

    KeyRequest(long id, int key, long scheduledNanos, long submitNanos) {
        this.id = id;
        this.key = key;
        this.scheduledNanos = scheduledNanos;
        this.submitNanos = submitNanos;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof KeyRequest request && key == request.key;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(key);
    }
}
