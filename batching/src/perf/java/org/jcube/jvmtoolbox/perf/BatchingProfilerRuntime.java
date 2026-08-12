package org.jcube.jvmtoolbox.perf;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.Recorder;
import org.jcube.jvmtoolbox.batching.BatchObserver;
import org.jcube.jvmtoolbox.batching.BatchOutcome;
import org.jcube.jvmtoolbox.batching.BatchingConfig;
import org.jcube.jvmtoolbox.batching.KeyBatchLoader;
import org.jcube.jvmtoolbox.batching.MicroBatcher;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.ClosedLoopExperiment;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Distribution;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Experiment;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.InputGenerator;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.KeyBackend;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.KeyedMetrics;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Latencies;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Lifecycle;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.MicroBackend;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.OpenLoopExperiment;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.OpenLoopStability;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.OpenLoopStatus;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Resources;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Result;
import org.jcube.jvmtoolbox.perf.BatchingProfiler.Target;

final class BatchingProfilerRuntime {
    private BatchingProfilerRuntime() {}

    static <I, O> BatchingProfiler.Result runMicro(
            BatchingProfiler.Lifecycle lifecycle,
            BatchingProfiler.Experiment experiment,
            int repetition,
            BatchingProfiler.InputGenerator<I> inputs,
            BatchingProfiler.MicroBackendFactory<I, O> backends)
            throws Exception {
        return run(
                lifecycle,
                experiment,
                BatchingProfiler.Target.MICRO,
                repetition,
                metrics -> {
                    BatchingProfiler.MicroBackend<I, O> backend = backends.open(experiment);
                    return new MicroSession<>(experiment.config(), inputs, backend, metrics);
                });
    }

    static <K, V> BatchingProfiler.Result runKeyed(
            BatchingProfiler.Lifecycle lifecycle,
            BatchingProfiler.Experiment experiment,
            int repetition,
            BatchingProfiler.InputGenerator<K> keys,
            BatchingProfiler.KeyBackendFactory<K, V> backends)
            throws Exception {
        return run(
                lifecycle,
                experiment,
                BatchingProfiler.Target.KEYED,
                repetition,
                metrics -> {
                    BatchingProfiler.KeyBackend<K, V> backend = backends.open(experiment);
                    return new KeyedSession<>(experiment.config(), keys, backend, metrics);
                });
    }

    private static Result run(
            Lifecycle lifecycle,
            Experiment experiment,
            Target target,
            int repetition,
            SessionFactory sessions)
            throws Exception {
        var metrics = new RunMetrics(experiment, target);
        try (Session session = sessions.open(metrics)) {
            runWorkload(experiment, lifecycle.warmupDuration(), metrics, session, null);
            var resources = new ResourceProbe();
            runWorkload(
                    experiment,
                    lifecycle.measurementDuration(),
                    metrics,
                    session,
                    () -> {
                        long started = System.nanoTime();
                        metrics.start(started);
                        resources.start();
                        return started;
                    });
            long ended = System.nanoTime();
            Resources resourceSnapshot = resources.finish(ended - metrics.startedNanos);
            return metrics.result(repetition, ended, resourceSnapshot);
        }
    }

    private static void runWorkload(
            Experiment experiment,
            Duration duration,
            RunMetrics metrics,
            Session session,
            MeasurementStart measurementStart)
            throws Exception {
        if (duration.isZero()) {
            return;
        }
        if (experiment instanceof ClosedLoopExperiment closed) {
            runClosedLoop(closed, duration, metrics, session, measurementStart);
        } else if (experiment instanceof OpenLoopExperiment open) {
            runOpenLoop(open, duration, metrics, session, measurementStart);
        }
    }

    private static void runClosedLoop(
            ClosedLoopExperiment experiment,
            Duration duration,
            RunMetrics metrics,
            Session session,
            MeasurementStart measurementStart)
            throws Exception {
        var ready = new CountDownLatch(experiment.foregroundConcurrency());
        var startGate = new CountDownLatch(1);
        var done = new CountDownLatch(experiment.foregroundConcurrency());
        var sequence = new AtomicLong();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int caller = 0; caller < experiment.foregroundConcurrency(); caller++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        startGate.await();
                        long deadline = System.nanoTime() + duration.toNanos();
                        while (System.nanoTime() < deadline) {
                            execute(sequence.getAndIncrement(), true, metrics, session);
                        }
                    } catch (Throwable failure) {
                        metrics.unexpected(failure);
                    } finally {
                        done.countDown();
                    }
                });
            }
            if (!ready.await(2, TimeUnit.MINUTES)) {
                throw new IllegalStateException("closed-loop callers did not start");
            }
            if (measurementStart != null) {
                measurementStart.start();
            }
            startGate.countDown();
            done.await();
        }
    }

    private static void runOpenLoop(
            OpenLoopExperiment experiment,
            Duration duration,
            RunMetrics metrics,
            Session session,
            MeasurementStart measurementStart)
            throws Exception {
        long count = Math.multiplyExact(
                        experiment.offeredRequestsPerSecond(), duration.toNanos())
                / 1_000_000_000L;
        var remaining = new AtomicLong(count);
        var drained = new CompletableFuture<Void>();
        if (count == 0) {
            drained.complete(null);
        }
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            long start = measurementStart == null
                    ? System.nanoTime()
                    : measurementStart.start();
            for (long sequence = 0; sequence < count; sequence++) {
                if (sequence == count / 2) {
                    metrics.markMidpoint();
                }
                long scheduled = start
                        + sequence * 1_000_000_000L / experiment.offeredRequestsPerSecond();
                waitUntil(scheduled);
                metrics.offered();
                long request = sequence;
                executor.execute(() -> executeAsync(
                        request, scheduled, metrics, session, remaining, drained));
            }
            metrics.markOfferEnd();
        }
        drained.get(2, TimeUnit.MINUTES);
    }

    private static void executeAsync(
            long sequence,
            long submittedNanos,
            RunMetrics metrics,
            Session session,
            AtomicLong remaining,
            CompletableFuture<Void> drained) {
        try {
            session.submit(sequence, submittedNanos).whenComplete((ignored, failure) -> {
                if (failure != null) {
                    metrics.unexpected(failure);
                }
                requestFinished(remaining, drained);
            });
        } catch (RejectedExecutionException | TimeoutException expected) {
            requestFinished(remaining, drained);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            metrics.unexpected(interrupted);
            requestFinished(remaining, drained);
        } catch (Throwable failure) {
            metrics.unexpected(failure);
            requestFinished(remaining, drained);
        }
    }

    private static void requestFinished(
            AtomicLong remaining, CompletableFuture<Void> drained) {
        if (remaining.decrementAndGet() == 0) {
            drained.complete(null);
        }
    }

    private static void execute(
            long sequence, boolean countOffered, RunMetrics metrics, Session session) {
        if (countOffered) {
            metrics.offered();
        }
        try {
            session.submit(sequence, System.nanoTime()).join();
        } catch (RejectedExecutionException | CompletionException expected) {
            Throwable cause = expected instanceof CompletionException ? expected.getCause() : expected;
            if (!(cause instanceof TimeoutException) && !(cause instanceof RejectedExecutionException)) {
                metrics.unexpected(cause);
            }
        } catch (TimeoutException expected) {
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            metrics.unexpected(interrupted);
        } catch (Throwable failure) {
            metrics.unexpected(failure);
        }
    }

    private static void waitUntil(long deadline) {
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            if (remaining > 50_000) {
                LockSupport.parkNanos(remaining - 20_000);
            } else {
                Thread.onSpinWait();
            }
        }
    }


    private interface MeasurementStart {
        long start();
    }

    private interface SessionFactory {
        Session open(RunMetrics metrics) throws Exception;
    }

    private interface Session extends AutoCloseable {
        CompletableFuture<Completion> submit(long sequence, long submittedNanos)
                throws InterruptedException, TimeoutException;
    }

    private enum Completion {
        SUCCESS,
        MISSING,
        FAILURE
    }

    private static final class MicroSession<I, O> implements Session {
        private final InputGenerator<I> inputs;
        private final MicroBackend<I, O> backend;
        private final RunMetrics metrics;
        private final MicroBatcher<TrackedInput<I>, O> batcher;

        private MicroSession(
                BatchingConfig config,
                InputGenerator<I> inputs,
                MicroBackend<I, O> backend,
                RunMetrics metrics) {
            this.inputs = inputs;
            this.backend = backend;
            this.metrics = metrics;
            batcher = new MicroBatcher<>(config, this::process, metrics);
        }

        @Override
        public CompletableFuture<Completion> submit(long sequence, long submittedNanos)
                throws InterruptedException, TimeoutException {
            var tracker = new Tracker(submittedNanos);
            var input = new TrackedInput<>(inputs.generate(sequence), tracker);
            CompletableFuture<O> future = metrics.submit(tracker, () -> batcher.submit(input));
            return future.handle((value, failure) -> metrics.complete(
                    tracker, failure == null ? Completion.SUCCESS : Completion.FAILURE));
        }

        private List<BatchOutcome<O>> process(List<TrackedInput<I>> batch) throws Exception {
            metrics.backendStarted();
            try {
                return backend.process(batch.stream().map(TrackedInput::value).toList());
            } finally {
                long available = System.nanoTime();
                for (TrackedInput<I> input : batch) {
                    input.tracker().backendAvailableNanos = available;
                }
                metrics.backendFinished(available);
            }
        }

        @Override
        public void close() throws Exception {
            batcher.close();
            backend.close();
        }
    }

    private static final class KeyedSession<K, V> implements Session {
        private final InputGenerator<K> keys;
        private final KeyBackend<K, V> backend;
        private final RunMetrics metrics;
        private final KeyBatchLoader<TrackedKey<K>, V> loader;

        private KeyedSession(
                BatchingConfig config,
                InputGenerator<K> keys,
                KeyBackend<K, V> backend,
                RunMetrics metrics) {
            this.keys = keys;
            this.backend = backend;
            this.metrics = metrics;
            loader = new KeyBatchLoader<>(config, this::load, metrics);
        }

        @Override
        public CompletableFuture<Completion> submit(long sequence, long submittedNanos)
                throws InterruptedException, TimeoutException {
            var tracker = new Tracker(submittedNanos);
            var key = new TrackedKey<>(keys.generate(sequence), tracker);
            CompletableFuture<Optional<V>> future = metrics.submit(tracker, () -> loader.load(key));
            return future.handle((value, failure) -> metrics.complete(
                    tracker,
                    failure != null
                            ? Completion.FAILURE
                            : value.isPresent() ? Completion.SUCCESS : Completion.MISSING));
        }

        private Map<TrackedKey<K>, BatchOutcome<V>> load(Set<TrackedKey<K>> batch)
                throws Exception {
            metrics.backendStarted();
            try {
                var logicalKeys = new LinkedHashSet<K>(batch.size());
                for (TrackedKey<K> key : batch) {
                    logicalKeys.add(key.key);
                }
                Map<K, BatchOutcome<V>> loaded = backend.load(Set.copyOf(logicalKeys));
                var results = new HashMap<TrackedKey<K>, BatchOutcome<V>>(loaded.size());
                for (TrackedKey<K> key : batch) {
                    if (loaded.containsKey(key.key)) {
                        results.put(key, loaded.get(key.key));
                    }
                }
                return results;
            } finally {
                long available = System.nanoTime();
                BatchTiming timing = batch.iterator().next().tracker.timing;
                for (Tracker tracker : timing.trackers) {
                    tracker.backendAvailableNanos = available;
                }
                metrics.backendFinished(available);
            }
        }

        @Override
        public void close() throws Exception {
            loader.close();
            backend.close();
        }
    }

    @FunctionalInterface
    private interface SubmitCall<T> {
        CompletableFuture<T> submit() throws InterruptedException, TimeoutException;
    }

    private record TrackedInput<I>(I value, Tracker tracker) {}

    private static final class TrackedKey<K> {
        private final K key;
        private final Tracker tracker;

        private TrackedKey(K key, Tracker tracker) {
            this.key = Objects.requireNonNull(key, "generated key");
            this.tracker = tracker;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof TrackedKey<?> tracked && key.equals(tracked.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }

    private static final class Tracker {
        private final long submittedNanos;
        private volatile long dispatchNanos;
        private volatile long backendAvailableNanos;
        private volatile BatchTiming timing;

        private Tracker(long submittedNanos) {
            this.submittedNanos = submittedNanos;
        }
    }

    private record BatchTiming(List<Tracker> trackers) {}

    private static final class RunMetrics implements BatchObserver {
        private final Experiment experiment;
        private final Target target;
        private final ThreadLocal<Tracker> submitting = new ThreadLocal<>();
        private final ConcurrentLinkedQueue<Tracker> admittedOrder = new ConcurrentLinkedQueue<>();
        private final LongAdder offered = new LongAdder();
        private final LongAdder admitted = new LongAdder();
        private final LongAdder rejected = new LongAdder();
        private final LongAdder timedOut = new LongAdder();
        private final LongAdder completed = new LongAdder();
        private final LongAdder successful = new LongAdder();
        private final LongAdder missing = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final LongAdder batches = new LongAdder();
        private final LongAdder dispatched = new LongAdder();
        private final LongAdder sizeTriggered = new LongAdder();
        private final LongAdder timeTriggered = new LongAdder();
        private final LongAdder keyedLogical = new LongAdder();
        private final LongAdder uniqueKeys = new LongAdder();
        private final AtomicInteger pending = new AtomicInteger();
        private final AtomicLong maxPending = new AtomicLong();
        private final AtomicReference<Throwable> unexpected = new AtomicReference<>();
        private final Recorder endToEnd = new Recorder(3);
        private final Recorder preDispatch = new Recorder(3);
        private final Recorder backend = new Recorder(3);
        private final Recorder completion = new Recorder(3);
        private final Object utilizationLock = new Object();
        private volatile boolean recording;
        private long startedNanos;
        private long elapsedNanos;
        private int backendActive;
        private int maxBackendActive;
        private long activeLastChanged;
        private long activeSlotNanos;
        private long midpointOutstanding = Long.MIN_VALUE;
        private long offerEndOutstanding = Long.MIN_VALUE;

        private RunMetrics(Experiment experiment, Target target) {
            this.experiment = experiment;
            this.target = target;
        }

        private void start(long now) {
            startedNanos = now;
            synchronized (utilizationLock) {
                activeLastChanged = now;
            }
            recording = true;
        }

        private void offered() {
            if (recording) {
                offered.increment();
            }
        }

        private <T> CompletableFuture<T> submit(Tracker tracker, SubmitCall<T> submit)
                throws InterruptedException, TimeoutException {
            submitting.set(tracker);
            try {
                return submit.submit();
            } finally {
                submitting.remove();
            }
        }

        private Completion complete(Tracker tracker, Completion outcome) {
            if (!recording) {
                return outcome;
            }
            long now = System.nanoTime();
            long available = tracker.backendAvailableNanos == 0
                    ? now
                    : tracker.backendAvailableNanos;
            completed.increment();
            switch (outcome) {
                case SUCCESS -> successful.increment();
                case MISSING -> missing.increment();
                case FAILURE -> failed.increment();
            }
            endToEnd.recordValue(Math.max(1, now - tracker.submittedNanos));
            backend.recordValue(Math.max(1, available - tracker.dispatchNanos));
            completion.recordValue(Math.max(1, now - available));
            return outcome;
        }

        private void backendStarted() {
            if (!recording) {
                return;
            }
            synchronized (utilizationLock) {
                updateUtilization(System.nanoTime());
                backendActive++;
                maxBackendActive = Math.max(maxBackendActive, backendActive);
            }
        }

        private void backendFinished(long now) {
            if (!recording) {
                return;
            }
            synchronized (utilizationLock) {
                updateUtilization(now);
                backendActive--;
            }
        }

        private void markMidpoint() {
            if (recording) {
                midpointOutstanding = outstanding();
            }
        }

        private void markOfferEnd() {
            if (recording) {
                offerEndOutstanding = outstanding();
            }
        }

        private long outstanding() {
            return offered.sum() - completed.sum() - rejected.sum() - timedOut.sum();
        }

        private void unexpected(Throwable failure) {
            unexpected.compareAndSet(null, failure);
        }

        @Override
        public void onAdmitted() {
            if (!recording) {
                return;
            }
            Tracker tracker = submitting.get();
            if (tracker == null) {
                unexpected(new IllegalStateException("admission was not correlated to a request"));
                return;
            }
            admitted.increment();
            admittedOrder.add(tracker);
            maxPending.accumulateAndGet(pending.incrementAndGet(), Math::max);
        }

        @Override
        public void onRejected(RejectionReason reason) {
            if (!recording) {
                return;
            }
            switch (reason) {
                case CAPACITY -> rejected.increment();
                case TIMEOUT -> timedOut.increment();
                case CLOSED -> unexpected(new IllegalStateException("submission rejected by closed batcher"));
            }
        }

        @Override
        public void onBatchDispatched(int requestCount) {
            if (!recording) {
                return;
            }
            long now = System.nanoTime();
            var trackers = new ArrayList<Tracker>(requestCount);
            for (int index = 0; index < requestCount; index++) {
                Tracker tracker = admittedOrder.poll();
                if (tracker == null) {
                    unexpected(new IllegalStateException("dispatch exceeded correlated admissions"));
                    return;
                }
                trackers.add(tracker);
            }
            var timing = new BatchTiming(List.copyOf(trackers));
            for (Tracker tracker : trackers) {
                tracker.dispatchNanos = now;
                tracker.timing = timing;
                preDispatch.recordValue(Math.max(1, now - tracker.submittedNanos));
            }
            batches.increment();
            dispatched.add(requestCount);
            if (requestCount == experiment.config().maxBatchSize()) {
                sizeTriggered.increment();
            } else {
                timeTriggered.increment();
            }
        }

        @Override
        public void onBatchCompleted(int requestCount, int successfulRequests, int failedRequests) {
            if (recording) {
                pending.addAndGet(-requestCount);
            }
        }

        @Override
        public void onBatchFailed(int requestCount, int failedRequests, Throwable failure) {
            if (recording) {
                pending.addAndGet(-requestCount);
            }
        }

        @Override
        public void onKeysCoalesced(int requestCount, int uniqueKeyCount) {
            if (recording) {
                keyedLogical.add(requestCount);
                uniqueKeys.add(uniqueKeyCount);
            }
        }

        private Result result(int repetition, long ended, Resources resources) {
            recording = false;
            synchronized (utilizationLock) {
                updateUtilization(ended);
            }
            elapsedNanos = Math.max(1, ended - startedNanos);
            Throwable failure = unexpected.get();
            if (failure != null) {
                throw new IllegalStateException("unexpected batching profiler failure", failure);
            }
            long admittedCount = admitted.sum();
            long completedCount = completed.sum();
            long maximumObservedPending = experiment.config().maxPendingRequests()
                    + (long) experiment.config().maxBatchSize()
                            * experiment.config().maxConcurrentBatches();
            if (admittedCount != completedCount
                    || completedCount != successful.sum() + missing.sum() + failed.sum()
                    || dispatched.sum() != admittedCount
                    || batches.sum() != sizeTriggered.sum() + timeTriggered.sum()
                    || offered.sum() != admittedCount + rejected.sum() + timedOut.sum()
                    || pending.get() != 0
                    || backendActive != 0
                    || maxPending.get() > maximumObservedPending
                    || maxBackendActive > experiment.config().maxConcurrentBatches()) {
                throw new IllegalStateException("batching profiler accounting invariant failed");
            }
            double utilization = (double) activeSlotNanos
                    / elapsedNanos
                    / experiment.config().maxConcurrentBatches();
            Optional<KeyedMetrics> keyed = target == Target.KEYED
                    ? Optional.of(new KeyedMetrics(
                            keyedLogical.sum(),
                            uniqueKeys.sum(),
                            keyedLogical.sum() - uniqueKeys.sum()))
                    : Optional.empty();
            Optional<OpenLoopStability> open = experiment instanceof OpenLoopExperiment
                    ? Optional.of(openLoopStability())
                    : Optional.empty();
            return new Result(
                    target,
                    experiment,
                    repetition,
                    offered.sum(),
                    admittedCount,
                    completedCount,
                    rejected.sum(),
                    timedOut.sum(),
                    failed.sum(),
                    successful.sum(),
                    missing.sum(),
                    batches.sum(),
                    dispatched.sum(),
                    sizeTriggered.sum(),
                    timeTriggered.sum(),
                    maxPending.get(),
                    maxBackendActive,
                    utilization,
                    elapsedNanos,
                    new Latencies(
                            new Distribution(endToEnd.getIntervalHistogram()),
                            new Distribution(preDispatch.getIntervalHistogram()),
                            new Distribution(backend.getIntervalHistogram()),
                            new Distribution(completion.getIntervalHistogram())),
                    keyed,
                    open,
                    resources);
        }

        private OpenLoopStability openLoopStability() {
            long midpoint = midpointOutstanding == Long.MIN_VALUE ? 0 : midpointOutstanding;
            long offerEnd = offerEndOutstanding == Long.MIN_VALUE ? 0 : offerEndOutstanding;
            long growth = offerEnd - midpoint;
            long allowed = (long) experiment.config().maxBatchSize()
                    * experiment.config().maxConcurrentBatches();
            OpenLoopStatus status = rejected.sum() == 0 && timedOut.sum() == 0 && growth <= allowed
                    ? OpenLoopStatus.STABLE
                    : OpenLoopStatus.BOUNDED_OVERLOAD;
            return new OpenLoopStability(status, midpoint, offerEnd, growth, allowed);
        }

        private void updateUtilization(long now) {
            activeSlotNanos += (now - activeLastChanged) * backendActive;
            activeLastChanged = now;
        }
    }

    private static final class ResourceProbe {
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final OperatingSystemMXBean operatingSystem =
                ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean bean
                        ? bean
                        : null;
        private final ThreadMXBean allocatedMemory =
                ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean ? bean : null;
        private long cpu;
        private long allocated;
        private long collections;
        private long collectionMillis;
        private long heap;

        private void start() {
            if (allocatedMemory != null
                    && allocatedMemory.isThreadAllocatedMemorySupported()
                    && !allocatedMemory.isThreadAllocatedMemoryEnabled()) {
                allocatedMemory.setThreadAllocatedMemoryEnabled(true);
            }
            cpu = operatingSystem == null ? -1 : operatingSystem.getProcessCpuTime();
            allocated = allocatedBytes();
            collections = gcCount();
            collectionMillis = gcMillis();
            heap = memory.getHeapMemoryUsage().getUsed();
        }

        private Resources finish(long elapsedNanos) {
            long endCpu = operatingSystem == null ? -1 : operatingSystem.getProcessCpuTime();
            return new Resources(
                    cpu < 0 || endCpu < 0 ? -1 : (double) (endCpu - cpu) / elapsedNanos,
                    difference(allocatedBytes(), allocated),
                    gcCount() - collections,
                    gcMillis() - collectionMillis,
                    heap,
                    memory.getHeapMemoryUsage().getUsed());
        }

        private long allocatedBytes() {
            return allocatedMemory == null || !allocatedMemory.isThreadAllocatedMemorySupported()
                    ? -1
                    : allocatedMemory.getTotalThreadAllocatedBytes();
        }

        private static long difference(long end, long start) {
            return end < 0 || start < 0 ? -1 : end - start;
        }

        private static long gcCount() {
            long count = 0;
            for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
                count += Math.max(0, collector.getCollectionCount());
            }
            return count;
        }

        private static long gcMillis() {
            long millis = 0;
            for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
                millis += Math.max(0, collector.getCollectionTime());
            }
            return millis;
        }
    }


}
