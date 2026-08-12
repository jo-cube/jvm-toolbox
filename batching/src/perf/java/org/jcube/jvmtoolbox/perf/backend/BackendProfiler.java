package org.jcube.jvmtoolbox.perf.backend;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

public final class BackendProfiler<I> {
    private static final int PREPARED_BATCHES_PER_WORKER = 32;

    private final Config config;
    private final InputGenerator<I> inputs;
    private final BackendFactory<I> backends;

    public BackendProfiler(
            Config config, InputGenerator<I> inputs, BackendFactory<I> backends) {
        this.config = Objects.requireNonNull(config, "config");
        this.inputs = Objects.requireNonNull(inputs, "inputs");
        this.backends = Objects.requireNonNull(backends, "backends");
    }

    public List<Result> profile() throws Exception {
        var results = new ArrayList<Result>();
        for (int batchSize : config.batchSizes()) {
            for (int concurrency : config.backendConcurrencies()) {
                List<List<List<I>>> prepared = prepare(batchSize, concurrency);
                for (int repetition = 1; repetition <= config.repetitions(); repetition++) {
                    try (BatchBackend<I> backend = backends.open(concurrency)) {
                        runPhase(
                                batchSize,
                                concurrency,
                                config.warmupDuration(),
                                prepared,
                                backend,
                                false);
                        Phase phase = runPhase(
                                batchSize,
                                concurrency,
                                config.measurementDuration(),
                                prepared,
                                backend,
                                true);
                        results.add(phase.result(batchSize, concurrency, repetition));
                    }
                }
            }
        }
        return List.copyOf(results);
    }

    public static List<AggregatedResult> aggregate(List<Result> repetitions) {
        Objects.requireNonNull(repetitions, "repetitions");
        Map<Configuration, List<Result>> groups = new LinkedHashMap<>();
        for (Result repetition : repetitions) {
            Objects.requireNonNull(repetition, "repetition");
            groups.computeIfAbsent(
                            new Configuration(
                                    repetition.batchSize(), repetition.backendConcurrency()),
                            ignored -> new ArrayList<>())
                    .add(repetition);
        }
        return groups.values().stream().map(AggregatedResult::from).toList();
    }

    private List<List<List<I>>> prepare(int batchSize, int concurrency) {
        var workers = new ArrayList<List<List<I>>>(concurrency);
        for (int worker = 0; worker < concurrency; worker++) {
            var batches = new ArrayList<List<I>>(PREPARED_BATCHES_PER_WORKER);
            for (int index = 0; index < PREPARED_BATCHES_PER_WORKER; index++) {
                List<I> batch = List.copyOf(Objects.requireNonNull(
                        inputs.generate(
                                batchSize,
                                (long) worker * PREPARED_BATCHES_PER_WORKER + index),
                        "generated batch"));
                if (batch.size() != batchSize) {
                    throw new IllegalArgumentException(
                            "input generator returned " + batch.size() + " inputs for batch size " + batchSize);
                }
                batches.add(batch);
            }
            workers.add(List.copyOf(batches));
        }
        return List.copyOf(workers);
    }

    private static <I> Phase runPhase(
            int batchSize,
            int concurrency,
            Duration duration,
            List<List<List<I>>> prepared,
            BatchBackend<I> backend,
            boolean measured)
            throws InterruptedException {
        var ready = new CountDownLatch(concurrency);
        var startGate = new CountDownLatch(1);
        var done = new CountDownLatch(concurrency);
        var phase = new Phase(measured);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int worker = 0; worker < concurrency; worker++) {
                List<List<I>> batches = prepared.get(worker);
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        startGate.await();
                        int index = 0;
                        while (System.nanoTime() < phase.deadlineNanos) {
                            phase.process(batchSize, batches.get(index++ % batches.size()), backend);
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            phase.start(duration);
            startGate.countDown();
            done.await();
            phase.finish();
            return phase;
        }
    }

    public record Config(
            List<Integer> batchSizes,
            List<Integer> backendConcurrencies,
            Duration warmupDuration,
            Duration measurementDuration,
            int repetitions) {

        public Config {
            batchSizes = List.copyOf(Objects.requireNonNull(batchSizes, "batchSizes"));
            backendConcurrencies =
                    List.copyOf(Objects.requireNonNull(backendConcurrencies, "backendConcurrencies"));
            Objects.requireNonNull(warmupDuration, "warmupDuration");
            Objects.requireNonNull(measurementDuration, "measurementDuration");
            if (batchSizes.isEmpty()
                    || backendConcurrencies.isEmpty()
                    || batchSizes.stream().anyMatch(value -> value == null || value < 1)
                    || backendConcurrencies.stream().anyMatch(value -> value == null || value < 1)
                    || warmupDuration.isNegative()
                    || measurementDuration.isZero()
                    || measurementDuration.isNegative()
                    || repetitions < 1) {
                throw new IllegalArgumentException("invalid backend profiler configuration");
            }
        }
    }

    @FunctionalInterface
    public interface InputGenerator<I> {
        List<I> generate(int batchSize, long batchSequence);
    }

    @FunctionalInterface
    public interface BackendFactory<I> {
        BatchBackend<I> open(int backendConcurrency) throws Exception;
    }

    @FunctionalInterface
    public interface BatchBackend<I> extends AutoCloseable {
        BatchResult process(List<I> batch) throws Exception;

        @Override
        default void close() throws Exception {}
    }

    public record BatchResult(int successful, int missing, int failed) {
        public BatchResult {
            if (successful < 0 || missing < 0 || failed < 0) {
                throw new IllegalArgumentException("batch result counts cannot be negative");
            }
        }

        public static BatchResult successful(int operations) {
            return new BatchResult(operations, 0, 0);
        }

        public int operations() {
            return successful + missing + failed;
        }
    }

    public record Result(
            int batchSize,
            int backendConcurrency,
            int repetition,
            long successfulOperations,
            long missingOperations,
            long failedOperations,
            long backendBatches,
            long failedBatches,
            int maximumObservedConcurrency,
            long elapsedNanos,
            Latency backendLatency,
            Resources resources) {

        public long logicalOperations() {
            return successfulOperations + missingOperations + failedOperations;
        }

        public double logicalOperationsPerSecond() {
            return logicalOperations() * 1_000_000_000d / elapsedNanos;
        }

        public double backendBatchesPerSecond() {
            return backendBatches * 1_000_000_000d / elapsedNanos;
        }

        public double errorRate() {
            return logicalOperations() == 0 ? 0 : (double) failedOperations / logicalOperations();
        }
    }

    public record AggregatedResult(
            int batchSize,
            int backendConcurrency,
            List<Result> repetitions,
            long successfulOperations,
            long missingOperations,
            long failedOperations,
            long backendBatches,
            long failedBatches,
            int maximumObservedConcurrency,
            long elapsedNanos,
            double medianLogicalOperationsPerSecond,
            Latency backendLatency,
            Stability stability) {

        public AggregatedResult {
            repetitions = List.copyOf(Objects.requireNonNull(repetitions, "repetitions"));
            Objects.requireNonNull(backendLatency, "backendLatency");
            Objects.requireNonNull(stability, "stability");
        }

        private static AggregatedResult from(List<Result> repetitions) {
            Result first = repetitions.getFirst();
            long successful = 0;
            long missing = 0;
            long failed = 0;
            long batches = 0;
            long failedBatches = 0;
            long elapsed = 0;
            int maximumConcurrency = 0;
            double[] throughputs = new double[repetitions.size()];
            for (int index = 0; index < repetitions.size(); index++) {
                Result result = repetitions.get(index);
                if (result.batchSize() != first.batchSize()
                        || result.backendConcurrency() != first.backendConcurrency()) {
                    throw new IllegalArgumentException("cannot aggregate different backend configurations");
                }
                successful += result.successfulOperations();
                missing += result.missingOperations();
                failed += result.failedOperations();
                batches += result.backendBatches();
                failedBatches += result.failedBatches();
                elapsed += result.elapsedNanos();
                maximumConcurrency = Math.max(
                        maximumConcurrency, result.maximumObservedConcurrency());
                throughputs[index] = result.logicalOperationsPerSecond();
            }
            Arrays.sort(throughputs);
            double median = median(throughputs);
            double minimum = throughputs[0];
            double maximum = throughputs[throughputs.length - 1];
            double relativeSpread = median == 0
                    ? minimum == maximum ? 0 : Double.POSITIVE_INFINITY
                    : (maximum - minimum) / Math.abs(median);
            return new AggregatedResult(
                    first.batchSize(),
                    first.backendConcurrency(),
                    repetitions,
                    successful,
                    missing,
                    failed,
                    batches,
                    failedBatches,
                    maximumConcurrency,
                    elapsed,
                    median,
                    Latency.merge(repetitions),
                    new Stability(minimum, median, maximum, relativeSpread));
        }

        public int repetitionCount() {
            return repetitions.size();
        }

        public long logicalOperations() {
            return successfulOperations + missingOperations + failedOperations;
        }

        public double logicalOperationsPerSecond() {
            return medianLogicalOperationsPerSecond;
        }

        public double combinedLogicalOperationsPerSecond() {
            return logicalOperations() * 1_000_000_000d / elapsedNanos;
        }

        public double backendBatchesPerSecond() {
            return backendBatches * 1_000_000_000d / elapsedNanos;
        }

        public double errorRate() {
            return logicalOperations() == 0 ? 0 : (double) failedOperations / logicalOperations();
        }
    }

    public static final class Latency {
        private final Histogram histogram;

        private Latency(Histogram histogram) {
            this.histogram = histogram.copy();
        }

        private static Latency merge(List<Result> repetitions) {
            Histogram merged = repetitions.getFirst().backendLatency().histogram.copy();
            for (int index = 1; index < repetitions.size(); index++) {
                merged.add(repetitions.get(index).backendLatency().histogram);
            }
            return new Latency(merged);
        }

        public long count() {
            return histogram.getTotalCount();
        }

        public double averageNanos() {
            return histogram.getMean();
        }

        public long p50() {
            return histogram.getValueAtPercentile(50);
        }

        public long p95() {
            return histogram.getValueAtPercentile(95);
        }

        public long p99() {
            return histogram.getValueAtPercentile(99);
        }

        public long p999() {
            return histogram.getValueAtPercentile(99.9);
        }

        public long max() {
            return histogram.getMaxValue();
        }
    }

    public record Stability(
            double minimumLogicalOperationsPerSecond,
            double medianLogicalOperationsPerSecond,
            double maximumLogicalOperationsPerSecond,
            double relativeSpread) {}

    public record Resources(
            double processCpuCores,
            long allocatedBytes,
            long gcCount,
            long gcMillis,
            long startHeapBytes,
            long endHeapBytes) {}

    private static final class Phase {
        private final Recorder latency;
        private final LongAdder successful = new LongAdder();
        private final LongAdder missing = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final LongAdder batches = new LongAdder();
        private final LongAdder failedBatches = new LongAdder();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicLong maximumActive = new AtomicLong();
        private final ResourceProbe resources = new ResourceProbe();
        private long startedNanos;
        private long deadlineNanos;
        private long elapsedNanos;

        private Phase(boolean measured) {
            latency = measured ? new Recorder(3) : null;
        }

        private void start(Duration duration) {
            startedNanos = System.nanoTime();
            deadlineNanos = startedNanos + duration.toNanos();
            if (latency != null) {
                resources.start();
            }
        }

        private <I> void process(int batchSize, List<I> batch, BatchBackend<I> backend) {
            long started = System.nanoTime();
            int concurrent = active.incrementAndGet();
            maximumActive.accumulateAndGet(concurrent, Math::max);
            try {
                BatchResult result = Objects.requireNonNull(backend.process(batch), "batch result");
                if (result.operations() != batchSize) {
                    throw new IllegalStateException(
                            "backend reported " + result.operations() + " operations for " + batchSize + " inputs");
                }
                successful.add(result.successful());
                missing.add(result.missing());
                failed.add(result.failed());
                if (result.failed() != 0) {
                    failedBatches.increment();
                }
            } catch (Exception failure) {
                failed.add(batchSize);
                failedBatches.increment();
            } finally {
                active.decrementAndGet();
                batches.increment();
                if (latency != null) {
                    latency.recordValue(Math.max(1, System.nanoTime() - started));
                }
            }
        }

        private void finish() {
            elapsedNanos = Math.max(1, System.nanoTime() - startedNanos);
        }

        private Result result(int batchSize, int concurrency, int repetition) {
            Histogram histogram = latency.getIntervalHistogram();
            return new Result(
                    batchSize,
                    concurrency,
                    repetition,
                    successful.sum(),
                    missing.sum(),
                    failed.sum(),
                    batches.sum(),
                    failedBatches.sum(),
                    (int) maximumActive.get(),
                    elapsedNanos,
                    new Latency(histogram),
                    resources.finish(elapsedNanos));
        }
    }

    private record Configuration(int batchSize, int backendConcurrency) {}

    private static double median(double[] sorted) {
        int middle = sorted.length / 2;
        return sorted.length % 2 == 0
                ? (sorted[middle - 1] + sorted[middle]) / 2
                : sorted[middle];
    }

    private static final class ResourceProbe {
        private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        private final OperatingSystemMXBean operatingSystem =
                ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean bean ? bean : null;
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
