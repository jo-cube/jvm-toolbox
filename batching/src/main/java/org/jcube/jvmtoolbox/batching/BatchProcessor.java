package org.jcube.jvmtoolbox.batching;

import java.util.List;

/**
 * Processes one positional batch. Implementations may be invoked concurrently up to the configured
 * limit.
 *
 * <p>The input list is ordered and unmodifiable. The returned list must contain exactly one non-null
 * outcome for every input, in the same order. Throwing fails the whole batch.
 *
 * <p>The batcher does not own or close the processor or any resources it captures. A processor must
 * not call back into the batcher that invoked it.
 *
 * @param <I> input type
 * @param <O> output type
 */
@FunctionalInterface
public interface BatchProcessor<I, O> {
    /**
     * Processes one formed batch.
     *
     * @param inputs ordered, unmodifiable batch inputs
     * @return one ordered outcome per input
     * @throws Exception to fail every still-live submission in the batch
     */
    List<BatchOutcome<O>> process(List<I> inputs) throws Exception;
}
