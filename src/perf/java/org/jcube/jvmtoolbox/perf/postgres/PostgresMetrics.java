package org.jcube.jvmtoolbox.perf.postgres;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.jcube.jvmtoolbox.batching.BatchObserver;

final class PostgresMetrics implements BatchObserver {
    private final LongAdder attempts = new LongAdder();
    private final LongAdder admitted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder timedOut = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder successful = new LongAdder();
    private final LongAdder missing = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder batches = new LongAdder();
    private final LongAdder logicalBatchKeys = new LongAdder();
    private final LongAdder uniqueBatchKeys = new LongAdder();
    private final LongAdder databaseBatches = new LongAdder();
    private final LongAdder databaseKeys = new LongAdder();
    private final LongAdder databaseErrors = new LongAdder();
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger databaseActive = new AtomicInteger();
    private final AtomicInteger activeCallers = new AtomicInteger();
    private final AtomicLong maxPending = new AtomicLong();
    private final AtomicLong maxInFlight = new AtomicLong();
    private final AtomicLong maxDatabaseActive = new AtomicLong();
    private final AtomicLong maxActiveCallers = new AtomicLong();
    private final AtomicReference<Throwable> unexpected = new AtomicReference<>();
    private final Recorder logicalLatency = new Recorder(3);
    private final Recorder databaseLatency = new Recorder(3);
    private final Recorder logicalBatchSizes = new Recorder(3);
    private final Recorder uniqueBatchSizes = new Recorder(3);
    private volatile long startNanos;
    private volatile boolean closed;

    void start(long now) {
        startNanos = now;
    }

    long elapsedNanos() {
        return System.nanoTime() - startNanos;
    }

    void attempt() {
        attempts.increment();
    }

    void callerStarted() {
        updateMax(maxActiveCallers, activeCallers.incrementAndGet());
    }

    void callerFinished() {
        activeCallers.decrementAndGet();
    }

    void logicalCompleted(long started, boolean found) {
        completed.increment();
        if (found) {
            successful.increment();
        } else {
            missing.increment();
        }
        logicalLatency.recordValue(Math.max(1, System.nanoTime() - started));
    }

    void logicalFailed(long started) {
        completed.increment();
        errors.increment();
        logicalLatency.recordValue(Math.max(1, System.nanoTime() - started));
    }

    void directBatchStarted(int logicalKeys, int uniqueKeys) {
        attempts.add(logicalKeys);
        admitted.add(logicalKeys);
        batches.increment();
        logicalBatchKeys.add(logicalKeys);
        uniqueBatchKeys.add(uniqueKeys);
        logicalBatchSizes.recordValue(logicalKeys);
    }

    void directBatchCompleted(long started, int successfulKeys, int missingKeys) {
        int logicalKeys = successfulKeys + missingKeys;
        completed.add(logicalKeys);
        successful.add(successfulKeys);
        missing.add(missingKeys);
        logicalLatency.recordValueWithCount(Math.max(1, System.nanoTime() - started), logicalKeys);
    }

    void directBatchFailed(long started, int logicalKeys) {
        completed.add(logicalKeys);
        errors.add(logicalKeys);
        logicalLatency.recordValueWithCount(Math.max(1, System.nanoTime() - started), logicalKeys);
    }

    void genericKeysDispatched(int uniqueKeys) {
        uniqueBatchKeys.add(uniqueKeys);
    }

    boolean hasUnexpected() {
        return unexpected.get() != null;
    }

    void databaseStarted() {
        updateMax(maxDatabaseActive, databaseActive.incrementAndGet());
    }

    void databaseFinished(int uniqueKeys, long started, boolean failed) {
        databaseActive.decrementAndGet();
        databaseBatches.increment();
        databaseKeys.add(uniqueKeys);
        if (failed) {
            databaseErrors.increment();
        }
        uniqueBatchSizes.recordValue(uniqueKeys);
        databaseLatency.recordValue(Math.max(1, System.nanoTime() - started));
    }

    void unexpected(Throwable failure) {
        unexpected.compareAndSet(null, failure);
    }

    @Override
    public void onAdmitted() {
        admitted.increment();
        updateMax(maxPending, pending.incrementAndGet());
    }

    @Override
    public void onRejected(RejectionReason reason) {
        if (reason == RejectionReason.TIMEOUT) {
            timedOut.increment();
        } else {
            rejected.increment();
        }
    }

    @Override
    public void onBatchDispatched(int requestCount) {
        updateMax(maxInFlight, inFlight.incrementAndGet());
        batches.increment();
        logicalBatchKeys.add(requestCount);
        logicalBatchSizes.recordValue(requestCount);
    }

    @Override
    public void onBatchCompleted(int requestCount, int successfulRequests, int failedRequests) {
        pending.addAndGet(-requestCount);
        inFlight.decrementAndGet();
    }

    @Override
    public void onBatchFailed(int requestCount, int failedRequests, Throwable failure) {
        pending.addAndGet(-requestCount);
        inFlight.decrementAndGet();
    }

    @Override
    public void onKeysCoalesced(int requestCount, int uniqueKeyCount) {
        uniqueBatchKeys.add(uniqueKeyCount);
    }

    @Override
    public void onClosed() {
        closed = true;
    }

    MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                attempts.sum(),
                admitted.sum(),
                rejected.sum(),
                timedOut.sum(),
                completed.sum(),
                successful.sum(),
                missing.sum(),
                errors.sum(),
                batches.sum(),
                logicalBatchKeys.sum(),
                uniqueBatchKeys.sum(),
                databaseBatches.sum(),
                databaseKeys.sum(),
                databaseErrors.sum(),
                maxPending.get(),
                maxInFlight.get(),
                maxDatabaseActive.get(),
                maxActiveCallers.get(),
                pending.get(),
                inFlight.get(),
                databaseActive.get(),
                closed,
                histogram(logicalLatency.getIntervalHistogram()),
                histogram(databaseLatency.getIntervalHistogram()),
                histogram(logicalBatchSizes.getIntervalHistogram()),
                histogram(uniqueBatchSizes.getIntervalHistogram()),
                unexpected.get());
    }

    MetricSample sample(double seconds, long heapBytes, int platformThreads) {
        return new MetricSample(
                seconds,
                attempts.sum(),
                completed.sum(),
                rejected.sum(),
                timedOut.sum(),
                pending.get(),
                inFlight.get(),
                databaseActive.get(),
                activeCallers.get(),
                heapBytes,
                platformThreads);
    }

    private static HistogramSnapshot histogram(Histogram histogram) {
        return new HistogramSnapshot(
                histogram.getTotalCount(),
                histogram.getMean(),
                histogram.getValueAtPercentile(50),
                histogram.getValueAtPercentile(95),
                histogram.getValueAtPercentile(99),
                histogram.getValueAtPercentile(99.9),
                histogram.getMaxValue());
    }

    private static void updateMax(AtomicLong maximum, long value) {
        maximum.accumulateAndGet(value, Math::max);
    }
}

record HistogramSnapshot(long count, double mean, long p50, long p95, long p99, long p999, long max) {}

record MetricsSnapshot(
        long attempts,
        long admitted,
        long rejected,
        long timedOut,
        long completed,
        long successful,
        long missing,
        long errors,
        long batches,
        long logicalBatchKeys,
        long uniqueBatchKeys,
        long databaseBatches,
        long databaseKeys,
        long databaseErrors,
        long maxPending,
        long maxInFlight,
        long maxDatabaseActive,
        long maxActiveCallers,
        int finalPending,
        int finalInFlight,
        int finalDatabaseActive,
        boolean closed,
        HistogramSnapshot logicalLatency,
        HistogramSnapshot databaseLatency,
        HistogramSnapshot logicalBatchSizes,
        HistogramSnapshot uniqueBatchSizes,
        Throwable unexpected) {}

record MetricSample(
        double seconds,
        long attempts,
        long completed,
        long rejected,
        long timedOut,
        int pending,
        int inFlight,
        int databaseActive,
        int activeCallers,
        long heapBytes,
        int platformThreads) {}

final class JvmProbe {
    private final PostgresMetrics metrics;
    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private final java.lang.management.ThreadMXBean platformThreads = ManagementFactory.getThreadMXBean();
    private final OperatingSystemMXBean operatingSystem =
            ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean bean ? bean : null;
    private final ThreadMXBean allocatedMemory =
            ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean ? bean : null;
    private final List<MetricSample> samples = new ArrayList<>();
    private final AtomicLong peakHeap = new AtomicLong();
    private volatile boolean running;
    private long startNanos;
    private long startCpu;
    private long startAllocated;
    private long startGcCount;
    private long startGcMillis;
    private long startHeap;
    private double systemCpuSum;
    private long systemCpuSamples;
    private Thread sampler;

    JvmProbe(PostgresMetrics metrics) {
        this.metrics = metrics;
        if (allocatedMemory != null
                && allocatedMemory.isThreadAllocatedMemorySupported()
                && !allocatedMemory.isThreadAllocatedMemoryEnabled()) {
            allocatedMemory.setThreadAllocatedMemoryEnabled(true);
        }
    }

    void start(long now) {
        startNanos = now;
        startCpu = operatingSystem == null ? -1 : operatingSystem.getProcessCpuTime();
        startAllocated = allocatedBytes();
        startGcCount = gcCount();
        startGcMillis = gcMillis();
        startHeap = heapUsed();
        peakHeap.set(startHeap);
        platformThreads.resetPeakThreadCount();
        running = true;
        sampler = Thread.ofPlatform().daemon(true).name("jvm-toolbox-postgres-sampler").start(this::sample);
    }

    ResourceUsage finish(long endNanos) {
        running = false;
        LockSupport.unpark(sampler);
        try {
            sampler.join();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        takeSample(endNanos);
        long elapsed = Math.max(1, endNanos - startNanos);
        long cpu = operatingSystem == null ? -1 : operatingSystem.getProcessCpuTime() - startCpu;
        return new ResourceUsage(
                elapsed,
                cpu < 0 ? -1 : (double) cpu / elapsed,
                systemCpuSamples == 0 ? -1 : systemCpuSum * 100 / systemCpuSamples,
                difference(allocatedBytes(), startAllocated),
                gcCount() - startGcCount,
                gcMillis() - startGcMillis,
                startHeap,
                heapUsed(),
                peakHeap.get(),
                platformThreads.getPeakThreadCount(),
                List.copyOf(samples));
    }

    private void sample() {
        while (running) {
            takeSample(System.nanoTime());
            LockSupport.parkNanos(100_000_000L);
        }
    }

    private void takeSample(long now) {
        long heap = heapUsed();
        peakHeap.accumulateAndGet(heap, Math::max);
        if (operatingSystem != null) {
            double cpu = operatingSystem.getCpuLoad();
            if (cpu >= 0) {
                systemCpuSum += cpu;
                systemCpuSamples++;
            }
        }
        samples.add(metrics.sample(
                Math.max(0, now - startNanos) / 1_000_000_000d,
                heap,
                platformThreads.getThreadCount()));
    }

    private long allocatedBytes() {
        return allocatedMemory == null || !allocatedMemory.isThreadAllocatedMemorySupported()
                ? -1
                : allocatedMemory.getTotalThreadAllocatedBytes();
    }

    private long heapUsed() {
        return memory.getHeapMemoryUsage().getUsed();
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

record ResourceUsage(
        long elapsedNanos,
        double processCpuCores,
        double systemCpuPercent,
        long allocatedBytes,
        long gcCount,
        long gcMillis,
        long startHeap,
        long endHeap,
        long peakHeap,
        int peakPlatformThreads,
        List<MetricSample> samples) {}
