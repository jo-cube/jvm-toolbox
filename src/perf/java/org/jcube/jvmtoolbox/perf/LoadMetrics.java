package org.jcube.jvmtoolbox.perf;

import com.sun.management.OperatingSystemMXBean;
import com.sun.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
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

final class LoadMetrics implements BatchObserver {
    enum Completion {
        SUCCESS,
        MISSING,
        FAILURE
    }

    private final PerfScenario scenario;
    private final LongAdder attempts = new LongAdder();
    private final LongAdder admitted = new LongAdder();
    private final LongAdder capacityRejected = new LongAdder();
    private final LongAdder timedOut = new LongAdder();
    private final LongAdder closedRejected = new LongAdder();
    private final LongAdder completed = new LongAdder();
    private final LongAdder successful = new LongAdder();
    private final LongAdder missing = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder batches = new LongAdder();
    private final LongAdder dispatchedRequests = new LongAdder();
    private final LongAdder sizeTriggered = new LongAdder();
    private final LongAdder timeTriggered = new LongAdder();
    private final LongAdder keyedRequests = new LongAdder();
    private final LongAdder uniqueKeys = new LongAdder();
    private final LongAdder[] phaseCompleted = {new LongAdder(), new LongAdder(), new LongAdder()};
    private final LongAdder[] phaseRejected = {new LongAdder(), new LongAdder(), new LongAdder()};
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger activeCallers = new AtomicInteger();
    private final AtomicInteger admissionCallers = new AtomicInteger();
    private final AtomicLong maxPending = new AtomicLong();
    private final AtomicLong maxInFlight = new AtomicLong();
    private final AtomicLong maxActiveCallers = new AtomicLong();
    private final AtomicLong maxAdmissionCallers = new AtomicLong();
    private final AtomicReference<Throwable> unexpected = new AtomicReference<>();
    private final Recorder endToEnd = new Recorder(3);
    private final Recorder admissionDelay = new Recorder(3);
    private final Recorder queueDelay = new Recorder(3);
    private final Recorder backendLatency = new Recorder(3);
    private final Recorder batchSizes = new Recorder(3);
    private final Recorder launchLag = new Recorder(3);
    private final Recorder[] phaseLatency = {new Recorder(3), new Recorder(3), new Recorder(3)};
    private volatile long startNanos;
    private volatile boolean closed;

    LoadMetrics(PerfScenario scenario) {
        this.scenario = scenario;
    }

    void start(long startNanos) {
        this.startNanos = startNanos;
    }

    long elapsedNanos(long nowNanos) {
        return nowNanos - startNanos;
    }

    long endNanos(Duration duration) {
        return startNanos + duration.toNanos();
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

    void admissionStarted() {
        updateMax(maxAdmissionCallers, admissionCallers.incrementAndGet());
    }

    void admissionFinished(long nanos) {
        admissionCallers.decrementAndGet();
        admissionDelay.recordValue(Math.max(1, nanos));
    }

    void recordLaunchLag(long nanos) {
        launchLag.recordValue(Math.max(1, nanos));
    }

    void recordQueueDelay(long nanos) {
        queueDelay.recordValue(Math.max(1, nanos));
    }

    void recordBackendLatency(long nanos) {
        backendLatency.recordValue(Math.max(1, nanos));
    }

    void completed(long scheduledNanos, Completion completion) {
        long now = System.nanoTime();
        long latency = Math.max(1, now - scheduledNanos);
        int phase = scenario.phase(now, startNanos);
        completed.increment();
        phaseCompleted[phase].increment();
        endToEnd.recordValue(latency);
        phaseLatency[phase].recordValue(latency);
        switch (completion) {
            case SUCCESS -> successful.increment();
            case MISSING -> missing.increment();
            case FAILURE -> failed.increment();
        }
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
        int phase = scenario.phase(System.nanoTime(), startNanos);
        phaseRejected[phase].increment();
        switch (reason) {
            case CAPACITY -> capacityRejected.increment();
            case TIMEOUT -> timedOut.increment();
            case CLOSED -> closedRejected.increment();
        }
    }

    @Override
    public void onBatchDispatched(int requestCount) {
        updateMax(maxInFlight, inFlight.incrementAndGet());
        batches.increment();
        dispatchedRequests.add(requestCount);
        batchSizes.recordValue(requestCount);
        if (requestCount == scenario.batchSize()) {
            sizeTriggered.increment();
        } else {
            timeTriggered.increment();
        }
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
        keyedRequests.add(requestCount);
        uniqueKeys.add(uniqueKeyCount);
    }

    @Override
    public void onClosed() {
        closed = true;
    }

    MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                attempts.sum(),
                admitted.sum(),
                capacityRejected.sum(),
                timedOut.sum(),
                closedRejected.sum(),
                completed.sum(),
                successful.sum(),
                missing.sum(),
                failed.sum(),
                batches.sum(),
                dispatchedRequests.sum(),
                sizeTriggered.sum(),
                timeTriggered.sum(),
                keyedRequests.sum(),
                uniqueKeys.sum(),
                maxPending.get(),
                maxInFlight.get(),
                maxActiveCallers.get(),
                maxAdmissionCallers.get(),
                pending.get(),
                inFlight.get(),
                closed,
                phaseCompleted[0].sum(),
                phaseCompleted[1].sum(),
                phaseCompleted[2].sum(),
                phaseRejected[0].sum(),
                phaseRejected[1].sum(),
                phaseRejected[2].sum(),
                histogram(endToEnd.getIntervalHistogram()),
                histogram(admissionDelay.getIntervalHistogram()),
                histogram(queueDelay.getIntervalHistogram()),
                histogram(backendLatency.getIntervalHistogram()),
                histogram(batchSizes.getIntervalHistogram()),
                histogram(launchLag.getIntervalHistogram()),
                histogram(phaseLatency[0].getIntervalHistogram()),
                histogram(phaseLatency[1].getIntervalHistogram()),
                histogram(phaseLatency[2].getIntervalHistogram()),
                unexpected.get());
    }

    MetricSample sample(double seconds, long heapBytes, int platformThreads) {
        return new MetricSample(
                seconds,
                attempts.sum(),
                completed.sum(),
                capacityRejected.sum(),
                timedOut.sum(),
                pending.get(),
                inFlight.get(),
                activeCallers.get(),
                admissionCallers.get(),
                heapBytes,
                platformThreads);
    }

    private static HistogramSnapshot histogram(Histogram histogram) {
        return new HistogramSnapshot(
                histogram.getTotalCount(),
                histogram.getMean(),
                histogram.getValueAtPercentile(50),
                histogram.getValueAtPercentile(90),
                histogram.getValueAtPercentile(95),
                histogram.getValueAtPercentile(99),
                histogram.getValueAtPercentile(99.9),
                histogram.getMaxValue());
    }

    private static void updateMax(AtomicLong maximum, long candidate) {
        maximum.accumulateAndGet(candidate, Math::max);
    }
}

record HistogramSnapshot(
        long count, double mean, long p50, long p90, long p95, long p99, long p999, long max) {}

record MetricsSnapshot(
        long attempts,
        long admitted,
        long capacityRejected,
        long timedOut,
        long closedRejected,
        long completed,
        long successful,
        long missing,
        long failed,
        long batches,
        long dispatchedRequests,
        long sizeTriggered,
        long timeTriggered,
        long keyedRequests,
        long uniqueKeys,
        long maxPending,
        long maxInFlight,
        long maxActiveCallers,
        long maxAdmissionCallers,
        int finalPending,
        int finalInFlight,
        boolean closed,
        long preCompleted,
        long slowCompleted,
        long recoveryCompleted,
        long preRejected,
        long slowRejected,
        long recoveryRejected,
        HistogramSnapshot endToEnd,
        HistogramSnapshot admissionDelay,
        HistogramSnapshot queueDelay,
        HistogramSnapshot backendLatency,
        HistogramSnapshot batchSizes,
        HistogramSnapshot launchLag,
        HistogramSnapshot preLatency,
        HistogramSnapshot slowLatency,
        HistogramSnapshot recoveryLatency,
        Throwable unexpected) {}

record MetricSample(
        double seconds,
        long attempts,
        long completed,
        long rejected,
        long timedOut,
        int pending,
        int inFlight,
        int activeCallers,
        int admissionCallers,
        long heapBytes,
        int platformThreads) {}

final class ResourceProbe {
    private final LoadMetrics metrics;
    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private final java.lang.management.ThreadMXBean platformThreads =
            ManagementFactory.getThreadMXBean();
    private final OperatingSystemMXBean operatingSystem =
            ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean bean
                    ? bean
                    : null;
    private final ThreadMXBean allocatedMemory =
            ManagementFactory.getThreadMXBean() instanceof ThreadMXBean bean ? bean : null;
    private final long baselineHeapBytes = heapUsed();
    private final List<MetricSample> samples = new ArrayList<>();
    private final AtomicLong peakHeapBytes = new AtomicLong(baselineHeapBytes);
    private volatile boolean running;
    private long startNanos;
    private long startCpuNanos;
    private long startAllocatedBytes;
    private long startGcCount;
    private long startGcMillis;
    private long startHeapBytes;
    private Thread sampler;

    ResourceProbe(LoadMetrics metrics) {
        this.metrics = metrics;
        if (allocatedMemory != null
                && allocatedMemory.isThreadAllocatedMemorySupported()
                && !allocatedMemory.isThreadAllocatedMemoryEnabled()) {
            allocatedMemory.setThreadAllocatedMemoryEnabled(true);
        }
    }

    void start(long startNanos) {
        this.startNanos = startNanos;
        startCpuNanos = operatingSystem == null ? -1 : operatingSystem.getProcessCpuTime();
        startAllocatedBytes = allocatedBytes();
        startGcCount = gcCount();
        startGcMillis = gcMillis();
        startHeapBytes = heapUsed();
        peakHeapBytes.accumulateAndGet(startHeapBytes, Math::max);
        platformThreads.resetPeakThreadCount();
        running = true;
        sampler = Thread.ofPlatform().daemon(true).name("jvm-toolbox-perf-sampler").start(this::sample);
    }

    ResourceUsage finish(long endNanos) {
        running = false;
        if (sampler != null) {
            LockSupport.unpark(sampler);
            try {
                sampler.join();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        takeSample(endNanos);
        long elapsed = Math.max(1, endNanos - startNanos);
        long cpuNanos = operatingSystem == null ? -1 : operatingSystem.getProcessCpuTime() - startCpuNanos;
        long allocated = difference(allocatedBytes(), startAllocatedBytes);
        double cpuCores = cpuNanos < 0 ? -1 : (double) cpuNanos / elapsed;
        double cpuPercent = cpuCores < 0
                ? -1
                : cpuCores * 100 / Runtime.getRuntime().availableProcessors();
        return new ResourceUsage(
                elapsed,
                cpuCores,
                cpuPercent,
                allocated,
                gcCount() - startGcCount,
                gcMillis() - startGcMillis,
                baselineHeapBytes,
                startHeapBytes,
                heapUsed(),
                peakHeapBytes.get(),
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
        peakHeapBytes.accumulateAndGet(heap, Math::max);
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
        double cpuCores,
        double cpuPercent,
        long allocatedBytes,
        long gcCount,
        long gcMillis,
        long baselineHeapBytes,
        long startHeapBytes,
        long endHeapBytes,
        long peakHeapBytes,
        int peakPlatformThreads,
        List<MetricSample> samples) {}
