package org.jcube.jvmtoolbox.perf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.HdrHistogram.Histogram;
import org.jcube.jvmtoolbox.batching.AdmissionPolicy;
import org.jcube.jvmtoolbox.batching.BatchOutcome;

public final class BatchingProfiler {
    private final Lifecycle lifecycle;
    private final List<Experiment> experiments;
    private final Target target;
    private final RunTarget runTarget;

    private BatchingProfiler(
            Lifecycle lifecycle,
            List<Experiment> experiments,
            Target target,
            RunTarget runTarget) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.experiments = List.copyOf(Objects.requireNonNull(experiments, "experiments"));
        this.target = Objects.requireNonNull(target, "target");
        this.runTarget = Objects.requireNonNull(runTarget, "runTarget");
        if (this.experiments.isEmpty()) {
            throw new IllegalArgumentException("at least one batching experiment is required");
        }
    }

    public static <I, O> BatchingProfiler micro(
            Lifecycle lifecycle,
            List<Experiment> experiments,
            InputGenerator<I> inputs,
            MicroBackendFactory<I, O> backends) {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(backends, "backends");
        return new BatchingProfiler(
                lifecycle,
                experiments,
                Target.MICRO,
                (experiment, repetition) -> BatchingProfilerRuntime.runMicro(
                        lifecycle,
                        experiment,
                        repetition,
                        inputs,
                        backends));
    }

    public static <K, V> BatchingProfiler keyed(
            Lifecycle lifecycle,
            List<Experiment> experiments,
            InputGenerator<K> keys,
            KeyBackendFactory<K, V> backends) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(backends, "backends");
        return new BatchingProfiler(
                lifecycle,
                experiments,
                Target.KEYED,
                (experiment, repetition) -> BatchingProfilerRuntime.runKeyed(
                        lifecycle,
                        experiment,
                        repetition,
                        keys,
                        backends));
    }

    public List<Result> profile() throws Exception {
        var results = new ArrayList<Result>(experiments.size() * lifecycle.repetitions());
        for (Experiment experiment : experiments) {
            for (int repetition = 1; repetition <= lifecycle.repetitions(); repetition++) {
                results.add(runTarget.run(experiment, repetition));
            }
        }
        return List.copyOf(results);
    }

    public static List<AggregatedResult> aggregate(List<Result> repetitions) {
        Objects.requireNonNull(repetitions, "repetitions");
        Map<Configuration, List<Result>> groups = new LinkedHashMap<>();
        for (Result result : repetitions) {
            Objects.requireNonNull(result, "repetition");
            groups.computeIfAbsent(
                            new Configuration(result.target(), result.experiment()),
                            ignored -> new ArrayList<>())
                    .add(result);
        }
        return groups.values().stream().map(AggregatedResult::from).toList();
    }

    public Target target() {
        return target;
    }

    public record Lifecycle(
            Duration warmupDuration, Duration measurementDuration, int repetitions) {
        public Lifecycle {
            Objects.requireNonNull(warmupDuration, "warmupDuration");
            Objects.requireNonNull(measurementDuration, "measurementDuration");
            if (warmupDuration.isNegative()
                    || measurementDuration.isZero()
                    || measurementDuration.isNegative()
                    || repetitions < 1) {
                throw new IllegalArgumentException("invalid batching profiler lifecycle");
            }
        }
    }

    public record BatchingConfiguration(
            int maxBatchSize,
            Duration maxWait,
            int maxConcurrentBatches,
            int maxPendingRequests) {
        public BatchingConfiguration {
            Objects.requireNonNull(maxWait, "maxWait");
            if (maxBatchSize < 1
                    || maxWait.isNegative()
                    || maxConcurrentBatches < 1
                    || maxPendingRequests < 1) {
                throw new IllegalArgumentException("invalid batching configuration");
            }
        }
    }

    public record Admission(AdmissionPolicy policy, Duration timeout) {
        public Admission {
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isNegative()) {
                throw new IllegalArgumentException("admission timeout cannot be negative");
            }
        }

        public static Admission reject() {
            return new Admission(AdmissionPolicy.REJECT, Duration.ZERO);
        }

        public static Admission waitIndefinitely() {
            return new Admission(AdmissionPolicy.WAIT, Duration.ZERO);
        }

        public static Admission waitFor(Duration timeout) {
            return new Admission(AdmissionPolicy.WAIT_WITH_TIMEOUT, timeout);
        }
    }

    public sealed interface Experiment permits ClosedLoopExperiment, OpenLoopExperiment {
        String name();

        BatchingConfiguration batching();

        Admission admission();
    }

    public record ClosedLoopExperiment(
            String name,
            BatchingConfiguration batching,
            Admission admission,
            int foregroundConcurrency)
            implements Experiment {
        public ClosedLoopExperiment {
            requireExperiment(name, batching, admission);
            if (foregroundConcurrency < 1) {
                throw new IllegalArgumentException("closed-loop concurrency must be positive");
            }
        }
    }

    public record OpenLoopExperiment(
            String name,
            BatchingConfiguration batching,
            Admission admission,
            long offeredRequestsPerSecond)
            implements Experiment {
        public OpenLoopExperiment {
            requireExperiment(name, batching, admission);
            if (offeredRequestsPerSecond < 1) {
                throw new IllegalArgumentException("open-loop offered rate must be positive");
            }
        }
    }

    private static void requireExperiment(
            String name, BatchingConfiguration batching, Admission admission) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(batching, "batching");
        Objects.requireNonNull(admission, "admission");
        if (name.isBlank()) {
            throw new IllegalArgumentException("experiment name cannot be blank");
        }
    }

    @FunctionalInterface
    public interface InputGenerator<I> {
        I generate(long sequence);
    }

    @FunctionalInterface
    public interface MicroBackendFactory<I, O> {
        MicroBackend<I, O> open(Experiment experiment) throws Exception;
    }

    @FunctionalInterface
    public interface KeyBackendFactory<K, V> {
        KeyBackend<K, V> open(Experiment experiment) throws Exception;
    }

    @FunctionalInterface
    public interface MicroBackend<I, O> extends AutoCloseable {
        List<BatchOutcome<O>> process(List<I> inputs) throws Exception;

        @Override
        default void close() throws Exception {}
    }

    @FunctionalInterface
    public interface KeyBackend<K, V> extends AutoCloseable {
        Map<K, BatchOutcome<V>> load(Set<K> keys) throws Exception;

        @Override
        default void close() throws Exception {}
    }

    public enum Target {
        MICRO,
        KEYED
    }

    public enum OpenLoopStatus {
        STABLE,
        BOUNDED_OVERLOAD
    }

    public record OpenLoopStability(
            OpenLoopStatus status,
            long midpointOutstanding,
            long offerEndOutstanding,
            long outstandingGrowth,
            long allowedGrowth) {}

    public record KeyedMetrics(
            long logicalRequests, long uniqueBackendKeys, long coalescedCallers) {
        public double coalescingRatio() {
            return logicalRequests == 0 ? 0 : (double) coalescedCallers / logicalRequests;
        }
    }

    public record Result(
            Target target,
            Experiment experiment,
            int repetition,
            long offeredRequests,
            long admittedRequests,
            long completedRequests,
            long rejectedRequests,
            long admissionTimeouts,
            long failedRequests,
            long successfulRequests,
            long missingRequests,
            long backendBatches,
            long dispatchedRequests,
            long sizeTriggeredBatches,
            long timeTriggeredBatches,
            long maximumPendingRequests,
            int maximumBackendConcurrency,
            double backendConcurrencyUtilization,
            long elapsedNanos,
            Latencies latencies,
            Optional<KeyedMetrics> keyedMetrics,
            Optional<OpenLoopStability> openLoopStability,
            Resources resources) {

        public double logicalRequestsPerSecond() {
            return completedRequests * 1_000_000_000d / elapsedNanos;
        }

        public double backendBatchesPerSecond() {
            return backendBatches * 1_000_000_000d / elapsedNanos;
        }

        public double averageBatchSize() {
            return backendBatches == 0 ? 0 : (double) dispatchedRequests / backendBatches;
        }

        public double batchFillRatio() {
            return averageBatchSize() / experiment.batching().maxBatchSize();
        }
    }

    public record AggregatedResult(
            Target target,
            Experiment experiment,
            List<Result> repetitions,
            long offeredRequests,
            long admittedRequests,
            long completedRequests,
            long rejectedRequests,
            long admissionTimeouts,
            long failedRequests,
            long successfulRequests,
            long missingRequests,
            long backendBatches,
            long dispatchedRequests,
            long sizeTriggeredBatches,
            long timeTriggeredBatches,
            long maximumPendingRequests,
            int maximumBackendConcurrency,
            double backendConcurrencyUtilization,
            long elapsedNanos,
            double medianLogicalRequestsPerSecond,
            Latencies latencies,
            Stability stability,
            Optional<KeyedMetrics> keyedMetrics,
            Optional<OpenLoopStability> openLoopStability) {

        private static AggregatedResult from(List<Result> repetitions) {
            Result first = repetitions.getFirst();
            long offered = 0;
            long admitted = 0;
            long completed = 0;
            long rejected = 0;
            long timeouts = 0;
            long failed = 0;
            long successful = 0;
            long missing = 0;
            long batches = 0;
            long dispatched = 0;
            long sizeTriggered = 0;
            long timeTriggered = 0;
            long maxPending = 0;
            int maxBackend = 0;
            long elapsed = 0;
            double utilizedNanos = 0;
            double[] throughputs = new double[repetitions.size()];
            long keyedLogical = 0;
            long uniqueKeys = 0;
            long coalesced = 0;
            boolean keyed = first.keyedMetrics().isPresent();
            OpenLoopStatus openStatus = OpenLoopStatus.STABLE;
            long midpointOutstanding = 0;
            long offerEndOutstanding = 0;
            long outstandingGrowth = Long.MIN_VALUE;
            long allowedGrowth = 0;
            for (int index = 0; index < repetitions.size(); index++) {
                Result result = repetitions.get(index);
                if (result.target() != first.target()
                        || !result.experiment().equals(first.experiment())) {
                    throw new IllegalArgumentException("cannot aggregate different batching experiments");
                }
                offered += result.offeredRequests();
                admitted += result.admittedRequests();
                completed += result.completedRequests();
                rejected += result.rejectedRequests();
                timeouts += result.admissionTimeouts();
                failed += result.failedRequests();
                successful += result.successfulRequests();
                missing += result.missingRequests();
                batches += result.backendBatches();
                dispatched += result.dispatchedRequests();
                sizeTriggered += result.sizeTriggeredBatches();
                timeTriggered += result.timeTriggeredBatches();
                maxPending = Math.max(maxPending, result.maximumPendingRequests());
                maxBackend = Math.max(maxBackend, result.maximumBackendConcurrency());
                utilizedNanos += result.backendConcurrencyUtilization() * result.elapsedNanos();
                elapsed += result.elapsedNanos();
                throughputs[index] = result.logicalRequestsPerSecond();
                if (keyed) {
                    KeyedMetrics keyedResult = result.keyedMetrics().orElseThrow();
                    keyedLogical += keyedResult.logicalRequests();
                    uniqueKeys += keyedResult.uniqueBackendKeys();
                    coalesced += keyedResult.coalescedCallers();
                }
                if (result.openLoopStability().isPresent()) {
                    OpenLoopStability open = result.openLoopStability().orElseThrow();
                    if (open.status() == OpenLoopStatus.BOUNDED_OVERLOAD) {
                        openStatus = OpenLoopStatus.BOUNDED_OVERLOAD;
                    }
                    midpointOutstanding = Math.max(midpointOutstanding, open.midpointOutstanding());
                    offerEndOutstanding = Math.max(offerEndOutstanding, open.offerEndOutstanding());
                    outstandingGrowth = Math.max(outstandingGrowth, open.outstandingGrowth());
                    allowedGrowth = Math.max(allowedGrowth, open.allowedGrowth());
                }
            }
            Arrays.sort(throughputs);
            double median = median(throughputs);
            double minimum = throughputs[0];
            double maximum = throughputs[throughputs.length - 1];
            double spread = median == 0
                    ? minimum == maximum ? 0 : Double.POSITIVE_INFINITY
                    : (maximum - minimum) / Math.abs(median);
            Optional<OpenLoopStability> openLoop = first.openLoopStability().isEmpty()
                    ? Optional.empty()
                    : Optional.of(new OpenLoopStability(
                            openStatus,
                            midpointOutstanding,
                            offerEndOutstanding,
                            outstandingGrowth,
                            allowedGrowth));
            return new AggregatedResult(
                    first.target(),
                    first.experiment(),
                    repetitions,
                    offered,
                    admitted,
                    completed,
                    rejected,
                    timeouts,
                    failed,
                    successful,
                    missing,
                    batches,
                    dispatched,
                    sizeTriggered,
                    timeTriggered,
                    maxPending,
                    maxBackend,
                    utilizedNanos / elapsed,
                    elapsed,
                    median,
                    Latencies.merge(repetitions),
                    new Stability(minimum, median, maximum, spread),
                    keyed
                            ? Optional.of(new KeyedMetrics(keyedLogical, uniqueKeys, coalesced))
                            : Optional.empty(),
                    openLoop);
        }

        public int repetitionCount() {
            return repetitions.size();
        }

        public double logicalRequestsPerSecond() {
            return medianLogicalRequestsPerSecond;
        }

        public double combinedLogicalRequestsPerSecond() {
            return completedRequests * 1_000_000_000d / elapsedNanos;
        }

        public double backendBatchesPerSecond() {
            return backendBatches * 1_000_000_000d / elapsedNanos;
        }

        public double averageBatchSize() {
            return backendBatches == 0 ? 0 : (double) dispatchedRequests / backendBatches;
        }

        public double batchFillRatio() {
            return averageBatchSize() / experiment.batching().maxBatchSize();
        }
    }

    public record Stability(
            double minimumLogicalRequestsPerSecond,
            double medianLogicalRequestsPerSecond,
            double maximumLogicalRequestsPerSecond,
            double relativeSpread) {}

    public record Latencies(
            Distribution endToEnd,
            Distribution preDispatch,
            Distribution backend,
            Distribution completion) {
        private static Latencies merge(List<Result> repetitions) {
            return new Latencies(
                    Distribution.merge(repetitions, result -> result.latencies().endToEnd()),
                    Distribution.merge(repetitions, result -> result.latencies().preDispatch()),
                    Distribution.merge(repetitions, result -> result.latencies().backend()),
                    Distribution.merge(repetitions, result -> result.latencies().completion()));
        }
    }

    public static final class Distribution {
        private final Histogram histogram;

        Distribution(Histogram histogram) {
            this.histogram = histogram.copy();
        }

        private static Distribution merge(
                List<Result> repetitions,
                java.util.function.Function<Result, Distribution> distribution) {
            Histogram merged = distribution.apply(repetitions.getFirst()).histogram.copy();
            for (int index = 1; index < repetitions.size(); index++) {
                merged.add(distribution.apply(repetitions.get(index)).histogram);
            }
            return new Distribution(merged);
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

    public record Resources(
            double processCpuCores,
            long allocatedBytes,
            long gcCount,
            long gcMillis,
            long startHeapBytes,
            long endHeapBytes) {}

    private interface RunTarget {
        Result run(Experiment experiment, int repetition) throws Exception;
    }

    private record Configuration(Target target, Experiment experiment) {}

    private static double median(double[] sorted) {
        int middle = sorted.length / 2;
        return sorted.length % 2 == 0
                ? (sorted[middle - 1] + sorted[middle]) / 2
                : sorted[middle];
    }
}
