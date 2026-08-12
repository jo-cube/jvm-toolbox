package org.jcube.jvmtoolbox.batching;

import java.util.Map;
import java.util.Set;

/**
 * Loads one batch of unique keys. Implementations may be invoked concurrently up to the configured
 * batch limit.
 *
 * <p>The key set is unmodifiable. The returned map may omit requested keys to mark them missing. Its
 * keys must be a subset of the requested keys, and each mapped outcome and successful value must be
 * non-null. Throwing fails the whole batch.
 *
 * <p>The loader does not own or close the processor or any resources it captures. A processor must not
 * call back into the loader that invoked it.
 *
 * @param <K> key type
 * @param <V> value type
 */
@FunctionalInterface
public interface KeyBatchProcessor<K, V> {
    /**
     * Loads values or per-key failures for one unique key set.
     *
     * @param keys unique, unmodifiable requested keys
     * @return mapped outcomes; omitted keys are missing
     * @throws Exception to fail every still-live caller represented by the batch
     */
    Map<K, BatchOutcome<V>> load(Set<K> keys) throws Exception;
}
