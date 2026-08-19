package org.jcube.jvmtoolbox.batching;

/**
 * Processes one completed accumulator state.
 *
 * <p>Calls are serialized in window order. The processor receives exclusive ownership of the state;
 * the {@link WindowedAccumulator} never accesses it again. Implementations may block, but must not
 * call back into the accumulator that invoked them. The accumulator does not own or close the
 * processor or resources captured by it.
 *
 * @param <A> accumulator state type
 */
@FunctionalInterface
public interface WindowProcessor<A> {
    /**
     * Processes one non-empty completed window.
     *
     * @param accumulated completed accumulator state
     * @throws Exception if processing fails
     */
    void process(A accumulated) throws Exception;
}
