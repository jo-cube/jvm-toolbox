package org.jcube.jvmtoolbox.batching;

import java.util.Objects;

/**
 * The success or failure of one position or key in a processed batch.
 *
 * <p>Outcomes are backend return values consumed by a batcher; they are not the foreground completion
 * type. A successful generic positional outcome may contain {@code null}. {@link KeyBatchLoader}
 * rejects successful {@code null} values because it uses {@link java.util.Optional} to represent
 * missing keys.
 *
 * @param <T> result value type
 */
public sealed interface BatchOutcome<T> {
    /**
     * Creates a successful positional result. The value may be {@code null}.
     *
     * @param value result value
     * @param <T> result value type
     * @return a successful outcome
     */
    static <T> BatchOutcome<T> success(T value) {
        return new Success<>(value);
    }

    /**
     * Creates a failed positional result.
     *
     * @param cause item failure
     * @param <T> result value type
     * @return a failed outcome
     */
    static <T> BatchOutcome<T> failure(Throwable cause) {
        return new Failure<>(cause);
    }

    /**
     * A successful positional result.
     *
     * @param value result value, which may be {@code null}
     * @param <T> result value type
     */
    record Success<T>(T value) implements BatchOutcome<T> {}

    /**
     * A failed positional result.
     *
     * @param cause item failure
     * @param <T> result value type
     */
    record Failure<T>(Throwable cause) implements BatchOutcome<T> {
        /**
         * Creates a failed outcome.
         *
         * @throws NullPointerException if {@code cause} is {@code null}
         */
        public Failure {
            Objects.requireNonNull(cause, "cause");
        }
    }
}
