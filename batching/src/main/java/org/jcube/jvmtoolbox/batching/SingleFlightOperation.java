package org.jcube.jvmtoolbox.batching;

import java.util.concurrent.CompletionStage;

/**
 * Starts one asynchronous operation for a {@link SingleFlight} key.
 *
 * <p>One caller in a new flight invokes this operation synchronously, so it should return its stage
 * promptly. Invocations for different keys may overlap. The single-flight instance does not own,
 * cancel, or close the returned stage or resources captured by the operation.
 *
 * @param <K> key type
 * @param <V> result type
 */
@FunctionalInterface
public interface SingleFlightOperation<K, V> {
    /**
     * Starts one operation for a non-null key.
     *
     * @param key operation key
     * @return a non-null stage; a successful result may be {@code null}
     * @throws Exception to fail the flight before a stage is returned
     */
    CompletionStage<? extends V> execute(K key) throws Exception;
}
