package org.jcube.jvmtoolbox.batching;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Incrementally combines contributions into count-or-time bounded windows.
 *
 * <p>This class is thread-safe. {@link #add(Object)} applies the accumulator on the admitting caller
 * under the window lock, so successful contributions have one total order and are included exactly
 * once. A completed state is transferred without copying to the processor, which is invoked
 * asynchronously on a library-owned thread. Processing is serialized in window order while callers
 * may form the next window. The primitive keeps no separate input reference after its accumulator
 * call returns.
 *
 * <p>The factory must return a distinct, non-null state for each window. Factory and accumulator
 * callbacks must return promptly, must not retain state for access outside their invocation, and must
 * not call back into this instance. If the accumulator throws, the contribution is not admitted;
 * when modifying an existing state it must leave that state usable and unchanged before throwing.
 *
 * <p>A processor failure is terminal for admission. Already admitted states are still passed to the
 * processor once in order, without retry, and the first failure is reported by the next operation and
 * by {@link #close()}. The processor may retain or transfer the state after return.
 *
 * @param <I> contribution type
 * @param <A> accumulator state type
 */
public final class WindowedAccumulator<I, A> implements AutoCloseable {
    private enum State {
        ACCEPTING,
        DRAINING,
        CLOSED
    }

    private final WindowedAccumulatorConfig config;
    private final Supplier<? extends A> factory;
    private final BiConsumer<? super A, ? super I> accumulator;
    private final WindowProcessor<? super A> processor;
    private final LongSupplier nanoClock;
    private final long maxWaitNanos;
    private final long admissionTimeoutNanos;
    // ponytail: one lock gives exact window boundaries; shard instances if measured contention demands it.
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workAvailable = lock.newCondition();
    private final Condition capacityAvailable = lock.newCondition();
    private final Condition windowProcessed = lock.newCondition();
    private final ArrayDeque<Window<A>> ready = new ArrayDeque<>();
    private final Thread coordinator;

    private State state = State.ACCEPTING;
    private A active;
    private int activeInputs;
    private long activeSince;
    private int pendingInputs;
    private long nextWindowId;
    private long processedWindowId;
    private boolean processing;
    private Throwable processingFailure;

    /**
     * Creates and starts a windowed accumulator.
     *
     * @param config window and admission limits
     * @param factory creates one mutable state per non-empty window
     * @param accumulator adds one contribution to the current state
     * @param processor processes completed states in order
     * @throws NullPointerException if an argument is {@code null}
     */
    public WindowedAccumulator(
            WindowedAccumulatorConfig config,
            Supplier<? extends A> factory,
            BiConsumer<? super A, ? super I> accumulator,
            WindowProcessor<? super A> processor) {
        this(config, factory, accumulator, processor, System::nanoTime);
    }

    WindowedAccumulator(
            WindowedAccumulatorConfig config,
            Supplier<? extends A> factory,
            BiConsumer<? super A, ? super I> accumulator,
            WindowProcessor<? super A> processor,
            LongSupplier nanoClock) {
        this.config = Objects.requireNonNull(config, "config");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.accumulator = Objects.requireNonNull(accumulator, "accumulator");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        maxWaitNanos = config.maxWait().toNanos();
        admissionTimeoutNanos = config.admissionTimeout().toNanos();
        coordinator = Thread.ofPlatform()
                .daemon(true)
                .name("jvm-toolbox-windowed-accumulator")
                .start(this::coordinate);
    }

    /**
     * Adds one non-null contribution.
     *
     * <p>The accumulator callback runs before this method returns. Processing is asynchronous. An
     * input whose accumulation completes successfully is admitted even if closing starts
     * concurrently.
     *
     * @param input contribution
     * @throws InterruptedException if interrupted while acquiring the lock or waiting for capacity
     * @throws TimeoutException if {@link AdmissionPolicy#WAIT_WITH_TIMEOUT} admission expires
     * @throws RejectedExecutionException if full under {@link AdmissionPolicy#REJECT}, closing, or a
     *     processor has failed
     * @throws NullPointerException if {@code input} is {@code null} or the factory returns {@code null}
     */
    public void add(I input) throws InterruptedException, TimeoutException {
        Objects.requireNonNull(input, "input");
        lock.lockInterruptibly();
        try {
            awaitCapacity();
            long now = nanoClock.getAsLong();
            if (activeInputs > 0 && remainingWait(activeSince, now, maxWaitNanos) == 0) {
                rotate();
            }

            A target = active;
            boolean newWindow = target == null;
            if (newWindow) {
                target = Objects.requireNonNull(factory.get(), "factory returned null");
            }
            accumulator.accept(target, input);
            if (newWindow) {
                active = target;
                activeSince = now;
            }
            activeInputs++;
            pendingInputs++;
            if (activeInputs == config.maxInputsPerWindow() || maxWaitNanos == 0) {
                rotate();
            } else {
                workAvailable.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Completes the current non-empty window and waits uninterruptibly for all earlier contributions
     * to finish processing.
     *
     * <p>The lock acquisition defines the boundary: successful additions ordered before it belong to
     * the flushed work; later additions belong to a following window. Empty windows are never emitted.
     * Interrupted status is restored before return. A processor callback must not invoke this method.
     *
     * @throws IllegalStateException after waiting if a processor has failed
     */
    public void flush() {
        lock.lock();
        try {
            rotate();
            long target = nextWindowId;
            awaitProcessed(target);
            throwIfProcessingFailed();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops admission, completes a partial window, and waits uninterruptibly for admitted work to
     * finish processing.
     *
     * <p>This method is idempotent and restores interrupted status before return. New contributions
     * are rejected once closing starts. A processor callback must not invoke this method.
     *
     * @throws IllegalStateException after draining if a processor has failed
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (state == State.ACCEPTING) {
                state = State.DRAINING;
                rotate();
                capacityAvailable.signalAll();
                workAvailable.signal();
            }
        } finally {
            lock.unlock();
        }
        if (Thread.currentThread() != coordinator) {
            joinCoordinator();
        }
        lock.lock();
        try {
            throwIfProcessingFailed();
        } finally {
            lock.unlock();
        }
    }

    void signalCoordinator() {
        lock.lock();
        try {
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    private void awaitCapacity() throws InterruptedException, TimeoutException {
        long remaining = admissionTimeoutNanos;
        while (pendingInputs == config.maxPendingInputs() && state == State.ACCEPTING) {
            switch (config.admissionPolicy()) {
                case REJECT -> throw new RejectedExecutionException(
                        "windowed accumulator capacity is exhausted");
                case WAIT -> capacityAvailable.await();
                case WAIT_WITH_TIMEOUT -> {
                    if (remaining <= 0) {
                        throw new TimeoutException("timed out waiting for accumulator capacity");
                    }
                    remaining = capacityAvailable.awaitNanos(remaining);
                }
            }
        }
        if (state != State.ACCEPTING) {
            var rejection = new RejectedExecutionException(processingFailure == null
                    ? "windowed accumulator is closing"
                    : "window processor failed");
            if (processingFailure != null) {
                rejection.initCause(processingFailure);
            }
            throw rejection;
        }
    }

    private void coordinate() {
        try {
            while (true) {
                Window<A> window = awaitWindow();
                if (window == null) {
                    return;
                }
                try {
                    Thread.startVirtualThread(() -> process(window));
                } catch (Throwable failure) {
                    finish(window, failure);
                }
            }
        } finally {
            lock.lock();
            try {
                state = State.CLOSED;
                capacityAvailable.signalAll();
                windowProcessed.signalAll();
                workAvailable.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    private Window<A> awaitWindow() {
        lock.lock();
        try {
            while (true) {
                if (state != State.ACCEPTING) {
                    rotate();
                }
                if (!processing && !ready.isEmpty()) {
                    processing = true;
                    return ready.removeFirst();
                }
                if (state != State.ACCEPTING
                        && activeInputs == 0
                        && ready.isEmpty()
                        && !processing) {
                    return null;
                }
                try {
                    if (state == State.ACCEPTING && activeInputs > 0) {
                        long remaining = remainingWait(
                                activeSince, nanoClock.getAsLong(), maxWaitNanos);
                        if (remaining == 0) {
                            rotate();
                        } else {
                            workAvailable.awaitNanos(remaining);
                        }
                    } else {
                        workAvailable.await();
                    }
                } catch (InterruptedException ignored) {
                    // The private coordinator has no cancellation contract.
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void process(Window<A> window) {
        Throwable failure = null;
        try {
            processor.process(window.accumulated);
        } catch (Throwable thrown) {
            failure = thrown;
        }
        finish(window, failure);
    }

    private void finish(Window<A> window, Throwable failure) {
        lock.lock();
        try {
            pendingInputs -= window.inputs;
            processedWindowId = window.id;
            processing = false;
            if (failure != null) {
                if (processingFailure == null) {
                    processingFailure = failure;
                }
                if (state == State.ACCEPTING) {
                    state = State.DRAINING;
                    rotate();
                }
            }
            capacityAvailable.signalAll();
            windowProcessed.signalAll();
            workAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    private void rotate() {
        if (activeInputs == 0) {
            return;
        }
        ready.addLast(new Window<>(++nextWindowId, active, activeInputs));
        active = null;
        activeInputs = 0;
        workAvailable.signal();
    }

    private void awaitProcessed(long target) {
        boolean interrupted = false;
        while (processedWindowId < target) {
            try {
                windowProcessed.await();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void throwIfProcessingFailed() {
        if (processingFailure != null) {
            throw new IllegalStateException("window processor failed", processingFailure);
        }
    }

    private void joinCoordinator() {
        boolean interrupted = false;
        while (coordinator.isAlive()) {
            try {
                coordinator.join();
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static long remainingWait(long oldest, long now, long maxWait) {
        return Math.max(0, maxWait - (now - oldest));
    }

    private static final class Window<A> {
        private final long id;
        private final A accumulated;
        private final int inputs;

        private Window(long id, A accumulated, int inputs) {
            this.id = id;
            this.accumulated = accumulated;
            this.inputs = inputs;
        }
    }
}
