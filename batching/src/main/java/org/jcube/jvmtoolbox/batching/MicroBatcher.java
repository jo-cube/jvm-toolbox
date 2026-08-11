package org.jcube.jvmtoolbox.batching;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Coalesces independent foreground inputs into positional backend batches.
 *
 * <p>This class is thread-safe. {@link #submit(Object)} may block during admission according to the
 * configured policy; processing and future completion are asynchronous after admission. Cancelling its
 * returned future affects only that submission and never interrupts or cancels a backend invocation.
 * Successful cancellation notifies the coordinator, while capacity is released only after the
 * cancellation is observed or its dispatched batch retires.
 * The batcher owns completion of returned futures; callers may observe, compose, wait for, or cancel
 * them, but must not complete them directly or forcibly replace their outcome.
 *
 * <p>{@link #close()} stops admission, immediately makes a partial batch eligible, and waits for all
 * admitted work and backend invocations to retire. New submissions are rejected once closing starts.
 * The batcher does not own or close its processor or resources captured by it.
 *
 * @param <I> input type
 * @param <O> output type
 */
public final class MicroBatcher<I, O> implements AutoCloseable {
    private enum State {
        ACCEPTING,
        DRAINING,
        CLOSED
    }

    private final BatchingConfig config;
    private final BatchProcessor<I, O> processor;
    private final BatchObserver observer;
    private final LongSupplier nanoClock;
    private final long maxWaitNanos;
    private final long admissionTimeoutNanos;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workAvailable = lock.newCondition();
    private final Condition capacityAvailable = lock.newCondition();
    private final ArrayDeque<Submission<I, O>> ingress = new ArrayDeque<>();
    private final CancellationNotifier cancellationNotifier;
    private final Semaphore executionSlots;
    private final Thread coordinator;

    private volatile State state = State.ACCEPTING;
    private int outstanding;

    /**
     * Creates and starts a batcher.
     *
     * @param config batching and admission limits
     * @param processor backend batch operation
     * @throws NullPointerException if either argument is {@code null}
     */
    public MicroBatcher(BatchingConfig config, BatchProcessor<I, O> processor) {
        this(config, processor, null, System::nanoTime);
    }

    /**
     * Creates and starts an observed batcher.
     *
     * @param config batching and admission limits
     * @param processor backend batch operation
     * @param observer lifecycle observer
     * @throws NullPointerException if any argument is {@code null}
     */
    public MicroBatcher(
            BatchingConfig config, BatchProcessor<I, O> processor, BatchObserver observer) {
        this(config, processor, Objects.requireNonNull(observer, "observer"), System::nanoTime);
    }

    MicroBatcher(BatchingConfig config, BatchProcessor<I, O> processor, LongSupplier nanoClock) {
        this(config, processor, null, nanoClock);
    }

    private MicroBatcher(
            BatchingConfig config,
            BatchProcessor<I, O> processor,
            BatchObserver observer,
            LongSupplier nanoClock) {
        this.config = Objects.requireNonNull(config, "config");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.observer = observer;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        maxWaitNanos = config.maxWait().toNanos();
        admissionTimeoutNanos = config.admissionTimeout().toNanos();
        cancellationNotifier = new CancellationNotifier(lock, workAvailable, observer);
        executionSlots = new Semaphore(config.maxConcurrentBatches());
        coordinator = Thread.ofPlatform()
                .daemon(true)
                .name("jvm-toolbox-micro-batcher")
                .start(this::coordinate);
    }

    /**
     * Admits one input and returns its independent completion handle.
     *
     * @param input non-null input
     * @return a cancellable future for this submission
     * @throws InterruptedException if interrupted while waiting for capacity
     * @throws TimeoutException if {@link AdmissionPolicy#WAIT_WITH_TIMEOUT} admission expires
     * @throws RejectedExecutionException if full under {@link AdmissionPolicy#REJECT}, or closing
     * @throws NullPointerException if {@code input} is {@code null}
     */
    public CompletableFuture<O> submit(I input) throws InterruptedException, TimeoutException {
        Objects.requireNonNull(input, "input");
        Submission<I, O> submission = null;
        BatchObserver.RejectionReason rejection;
        lock.lockInterruptibly();
        try {
            rejection = awaitCapacity();
            if (rejection == null) {
                submission = new Submission<>(
                        input, nanoClock.getAsLong(), new OwnedFuture<>(cancellationNotifier));
                outstanding++;
                if (observer != null) {
                    BatchObserverSupport.admitted(observer);
                }
                ingress.addLast(submission);
                workAvailable.signal();
            }
        } finally {
            lock.unlock();
        }
        if (rejection != null) {
            if (observer != null) {
                BatchObserverSupport.rejected(observer, rejection);
            }
            if (rejection == BatchObserver.RejectionReason.TIMEOUT) {
                throw new TimeoutException("timed out waiting for batcher capacity");
            }
            throw new RejectedExecutionException(
                    rejection == BatchObserver.RejectionReason.CLOSED
                            ? "batcher is closing"
                            : "batcher capacity is exhausted");
        }
        return submission.future;
    }

    /**
     * Stops admission and waits uninterruptibly for admitted work to drain.
     *
     * <p>This method is idempotent. If the calling thread is interrupted while waiting, draining still
     * completes and its interrupted status is restored before return. Do not invoke it from this
     * batcher's processor or observer callbacks.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (state == State.ACCEPTING) {
                state = State.DRAINING;
                capacityAvailable.signalAll();
                workAvailable.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (Thread.currentThread() != coordinator) {
            joinCoordinator();
        }
    }

    void signalCoordinator() {
        cancellationNotifier.signal();
    }

    private BatchObserver.RejectionReason awaitCapacity() throws InterruptedException {
        long remaining = admissionTimeoutNanos;
        while (outstanding == config.maxPendingRequests() && state == State.ACCEPTING) {
            switch (config.admissionPolicy()) {
                case REJECT -> {
                    return BatchObserver.RejectionReason.CAPACITY;
                }
                case WAIT -> capacityAvailable.await();
                case WAIT_WITH_TIMEOUT -> {
                    if (remaining <= 0) {
                        return BatchObserver.RejectionReason.TIMEOUT;
                    }
                    remaining = capacityAvailable.awaitNanos(remaining);
                }
            }
        }
        return state == State.ACCEPTING ? null : BatchObserver.RejectionReason.CLOSED;
    }

    private void coordinate() {
        try {
            while (true) {
                var first = takeFirst();
                if (first == null) {
                    if (state != State.ACCEPTING) {
                        executionSlots.acquireUninterruptibly(config.maxConcurrentBatches());
                        executionSlots.release(config.maxConcurrentBatches());
                        return;
                    }
                    continue;
                }
                if (first.future.isCancelled()) {
                    retire(first);
                    continue;
                }
                dispatch(formBatch(first));
            }
        } finally {
            lock.lock();
            try {
                state = State.CLOSED;
                capacityAvailable.signalAll();
                workAvailable.signalAll();
            } finally {
                lock.unlock();
            }
            if (observer != null) {
                BatchObserverSupport.closed(observer);
            }
        }
    }

    private Submission<I, O> takeFirst() {
        lock.lock();
        try {
            while (ingress.isEmpty()) {
                if (state != State.ACCEPTING) {
                    return null;
                }
                try {
                    workAvailable.await();
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return ingress.removeFirst();
        } finally {
            lock.unlock();
        }
    }

    private List<Submission<I, O>> formBatch(Submission<I, O> first) {
        var batch = new ArrayList<Submission<I, O>>(config.maxBatchSize());
        batch.add(first);
        while (batch.size() < config.maxBatchSize()) {
            var next = takeBeforeDeadline(batch.getFirst().admittedAt, batch);
            if (next == null) {
                if (!hasCancelled(batch)) {
                    return batch;
                }
                retireCancelled(batch);
                if (batch.isEmpty()) {
                    return batch;
                }
                continue;
            }
            if (next.future.isCancelled()) {
                retire(next);
            } else {
                batch.add(next);
            }
        }
        return batch;
    }

    private Submission<I, O> takeBeforeDeadline(
            long oldestAdmission, List<Submission<I, O>> batch) {
        lock.lock();
        try {
            while (ingress.isEmpty()) {
                if (hasCancelled(batch)) {
                    return null;
                }
                if (state != State.ACCEPTING) {
                    return null;
                }
                long remaining = remainingWait(oldestAdmission, nanoClock.getAsLong(), maxWaitNanos);
                if (remaining == 0) {
                    return null;
                }
                try {
                    workAvailable.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return ingress.removeFirst();
        } finally {
            lock.unlock();
        }
    }

    private static boolean hasCancelled(List<? extends Submission<?, ?>> batch) {
        for (var submission : batch) {
            if (submission.future.isCancelled()) {
                return true;
            }
        }
        return false;
    }

    private void dispatch(List<Submission<I, O>> batch) {
        var live = new ArrayList<>(batch);
        retireCancelled(live);
        if (live.isEmpty()) {
            return;
        }

        executionSlots.acquireUninterruptibly();
        retireCancelled(live);
        if (live.isEmpty()) {
            executionSlots.release();
            return;
        }
        if (observer != null) {
            BatchObserverSupport.batchDispatched(observer, live.size());
        }
        try {
            Thread.startVirtualThread(() -> process(live));
        } catch (Throwable failure) {
            if (observer == null) {
                fail(live, failure);
            } else {
                int failedRequests = failObserved(live, failure);
                BatchObserverSupport.batchFailed(observer, live.size(), failedRequests, failure);
            }
            retire(live);
            executionSlots.release();
        }
    }

    private void retireCancelled(List<Submission<I, O>> batch) {
        var iterator = batch.iterator();
        while (iterator.hasNext()) {
            var submission = iterator.next();
            if (submission.future.isCancelled()) {
                iterator.remove();
                retire(submission);
            }
        }
    }

    private void process(List<Submission<I, O>> batch) {
        try {
            var inputs = new ArrayList<I>(batch.size());
            for (var submission : batch) {
                inputs.add(submission.input);
            }
            List<BatchOutcome<O>> outcomes = processor.process(List.copyOf(inputs));
            validateOutcomes(outcomes, batch.size());
            if (observer == null) {
                complete(batch, outcomes);
            } else {
                completeObserved(batch, outcomes);
            }
        } catch (Throwable failure) {
            if (observer == null) {
                fail(batch, failure);
            } else {
                int failedRequests = failObserved(batch, failure);
                BatchObserverSupport.batchFailed(observer, batch.size(), failedRequests, failure);
            }
        } finally {
            retire(batch);
            executionSlots.release();
        }
    }

    private static void validateOutcomes(List<?> outcomes, int expectedSize) {
        if (outcomes == null) {
            throw new IllegalStateException("batch processor returned null");
        }
        if (outcomes.size() != expectedSize) {
            throw new IllegalStateException(
                    "batch processor returned " + outcomes.size() + " outcomes for " + expectedSize + " inputs");
        }
        for (var outcome : outcomes) {
            if (outcome == null) {
                throw new IllegalStateException("batch processor returned a null outcome");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <I, O> void complete(
            List<Submission<I, O>> batch, List<BatchOutcome<O>> outcomes) {
        for (int index = 0; index < batch.size(); index++) {
            var future = batch.get(index).future;
            var outcome = outcomes.get(index);
            if (outcome instanceof BatchOutcome.Success<?> success) {
                future.complete((O) success.value());
            } else if (outcome instanceof BatchOutcome.Failure<?> failure) {
                future.completeExceptionally(failure.cause());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void completeObserved(
            List<Submission<I, O>> batch, List<BatchOutcome<O>> outcomes) {
        int successfulRequests = 0;
        int failedRequests = 0;
        for (int index = 0; index < batch.size(); index++) {
            var future = batch.get(index).future;
            var outcome = outcomes.get(index);
            if (outcome instanceof BatchOutcome.Success<?> success) {
                if (future.complete((O) success.value())) {
                    successfulRequests++;
                }
            } else if (outcome instanceof BatchOutcome.Failure<?> failure
                    && future.completeExceptionally(failure.cause())) {
                failedRequests++;
            }
        }
        BatchObserverSupport.batchCompleted(
                observer, batch.size(), successfulRequests, failedRequests);
    }

    private static void fail(List<? extends Submission<?, ?>> batch, Throwable failure) {
        for (var submission : batch) {
            submission.future.completeExceptionally(failure);
        }
    }

    private static int failObserved(
            List<? extends Submission<?, ?>> batch, Throwable failure) {
        int failedRequests = 0;
        for (var submission : batch) {
            if (submission.future.completeExceptionally(failure)) {
                failedRequests++;
            }
        }
        return failedRequests;
    }

    private void retire(List<? extends Submission<?, ?>> batch) {
        for (var submission : batch) {
            retire(submission);
        }
    }

    private void retire(Submission<?, ?> submission) {
        lock.lock();
        try {
            outstanding--;
            capacityAvailable.signal();
        } finally {
            lock.unlock();
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

    private static final class Submission<I, O> {
        private final I input;
        private final long admittedAt;
        private final CompletableFuture<O> future;

        private Submission(I input, long admittedAt, CompletableFuture<O> future) {
            this.input = input;
            this.admittedAt = admittedAt;
            this.future = future;
        }
    }

    private static final class OwnedFuture<O> extends CompletableFuture<O> {
        private final CancellationNotifier notifier;

        private OwnedFuture(CancellationNotifier notifier) {
            this.notifier = notifier;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled) {
                notifier.cancelled();
            }
            return cancelled;
        }
    }

    private static final class CancellationNotifier {
        private final ReentrantLock lock;
        private final Condition workAvailable;
        private final BatchObserver observer;

        private CancellationNotifier(
                ReentrantLock lock, Condition workAvailable, BatchObserver observer) {
            this.lock = lock;
            this.workAvailable = workAvailable;
            this.observer = observer;
        }

        private void cancelled() {
            if (observer != null) {
                BatchObserverSupport.cancelled(observer);
            }
            signal();
        }

        private void signal() {
            lock.lock();
            try {
                workAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}
