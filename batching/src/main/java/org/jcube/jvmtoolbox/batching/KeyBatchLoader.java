package org.jcube.jvmtoolbox.batching;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Coalesces equal keys within each formed batch and fans one backend outcome out to their callers.
 *
 * <p>Coalescing ends when a batch is dispatched: a later request may invoke the backend again even
 * while an earlier batch is still running. This loader has no persistent cache. Configuration batch
 * size and pending capacity count caller submissions, not unique backend keys.
 *
 * <p>A successful empty {@link Optional} means missing. A failed key or whole-batch failure completes
 * its future exceptionally. Cancelling one future does not cancel work shared with another caller.
 * Successful cancellation notifies the coordinator, while capacity is released only after the
 * cancellation is observed or its dispatched batch retires.
 * A batch with a terminal outcome releases pending capacity before its futures are completed, so a
 * short, non-blocking dependent action may admit follow-up work even when capacity was full.
 * Synchronous dependent actions still occupy that batch's backend-concurrency slot until they return.
 * The loader owns completion of returned futures; callers may observe, compose, wait for, or cancel
 * them, but must not complete them directly or forcibly replace their outcome.
 * This class is thread-safe; loading is asynchronous after admission, while admission itself may block
 * according to the configured policy.
 *
 * @param <K> key type
 * @param <V> non-null value type
 */
public final class KeyBatchLoader<K, V> implements AutoCloseable {
    private final MicroBatcher<K, Optional<V>> batcher;

    /**
     * Creates and starts a keyed loader.
     *
     * @param config batching and admission limits
     * @param processor mapped backend operation
     * @throws NullPointerException if either argument is {@code null}
     */
    public KeyBatchLoader(BatchingConfig config, KeyBatchProcessor<K, V> processor) {
        Objects.requireNonNull(processor, "processor");
        batcher = new MicroBatcher<>(config, keys -> process(keys, processor, null));
    }

    /**
     * Creates and starts an observed keyed loader.
     *
     * @param config batching and admission limits
     * @param processor mapped backend operation
     * @param observer shared batching and keyed-coalescing observer
     * @throws NullPointerException if any argument is {@code null}
     */
    public KeyBatchLoader(
            BatchingConfig config, KeyBatchProcessor<K, V> processor, BatchObserver observer) {
        Objects.requireNonNull(processor, "processor");
        Objects.requireNonNull(observer, "observer");
        batcher = new MicroBatcher<>(
                config, keys -> process(keys, processor, observer), observer);
    }

    /**
     * Admits one non-null key and returns its independent completion handle.
     *
     * @param key key to load
     * @return a cancellable future containing the value or empty when missing
     * @throws InterruptedException if interrupted while waiting for capacity
     * @throws TimeoutException if {@link AdmissionPolicy#WAIT_WITH_TIMEOUT} admission expires
     * @throws RejectedExecutionException if full under {@link AdmissionPolicy#REJECT}, or closing
     * @throws NullPointerException if {@code key} is {@code null}
     */
    public CompletableFuture<Optional<V>> load(K key) throws InterruptedException, TimeoutException {
        return batcher.submit(key);
    }

    /**
     * Stops admission and waits uninterruptibly for admitted loads to drain.
     *
     * <p>This method is idempotent and restores the calling thread's interrupted status after draining.
     * The loader does not close its processor or resources captured by it. Do not invoke this method
     * from this loader's processor, observer callbacks, or synchronous dependent actions on returned
     * futures.
     */
    @Override
    public void close() {
        batcher.close();
    }

    @SuppressWarnings("unchecked")
    private static <K, V> List<BatchOutcome<Optional<V>>> process(
            List<K> keys, KeyBatchProcessor<K, V> processor, BatchObserver observer) throws Exception {
        Set<K> uniqueKeys = Set.copyOf(keys);
        if (observer != null) {
            BatchObserverSupport.keysCoalesced(observer, keys.size(), uniqueKeys.size());
        }
        Map<K, BatchOutcome<V>> loaded = processor.load(uniqueKeys);
        validate(loaded, uniqueKeys);

        var outcomes = new ArrayList<BatchOutcome<Optional<V>>>(keys.size());
        for (K key : keys) {
            if (!loaded.containsKey(key)) {
                outcomes.add(BatchOutcome.success(Optional.empty()));
                continue;
            }
            BatchOutcome<V> outcome = loaded.get(key);
            if (outcome instanceof BatchOutcome.Success<?> success) {
                outcomes.add(BatchOutcome.success(Optional.of((V) success.value())));
            } else if (outcome instanceof BatchOutcome.Failure<?> failure) {
                outcomes.add(BatchOutcome.failure(failure.cause()));
            }
        }
        return outcomes;
    }

    private static <K, V> void validate(Map<K, BatchOutcome<V>> loaded, Set<K> requested) {
        if (loaded == null) {
            throw new IllegalStateException("key batch processor returned null");
        }
        for (var entry : loaded.entrySet()) {
            if (entry.getKey() == null || !requested.contains(entry.getKey())) {
                throw new IllegalStateException("key batch processor returned an unrequested key");
            }
            BatchOutcome<V> outcome = entry.getValue();
            if (outcome == null) {
                throw new IllegalStateException("key batch processor returned a null outcome");
            }
            if (outcome instanceof BatchOutcome.Success<?> success && success.value() == null) {
                throw new IllegalStateException("key batch processor returned a null value");
            }
        }
    }
}
