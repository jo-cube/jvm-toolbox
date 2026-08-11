package org.jcube.jvmtoolbox.perf;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import org.jcube.jvmtoolbox.batching.AdmissionPolicy;
import org.jcube.jvmtoolbox.batching.BatchOutcome;
import org.jcube.jvmtoolbox.batching.BatchingConfig;
import org.jcube.jvmtoolbox.perf.backend.ParetoFrontier;

public final class BatchingProfilerHarness {
    private static final Path REPORT =
            Path.of("build", "reports", "perf", "batching-synthetic-results.csv");
    private static final BatchingProfiler.Lifecycle LIFECYCLE =
            new BatchingProfiler.Lifecycle(
                    Duration.ofMillis(100), Duration.ofMillis(400), 3);

    private BatchingProfilerHarness() {}

    public static void main(String[] args) throws Exception {
        List<BatchingProfiler.Experiment> microExperiments = microExperiments();
        BatchingProfiler micro = BatchingProfiler.micro(
                LIFECYCLE,
                microExperiments,
                sequence -> sequence,
                experiment -> inputs -> {
                    delay(backendLatency(experiment));
                    return inputs.stream().map(BatchOutcome::success).toList();
                });
        List<BatchingProfiler.Result> repetitions = new ArrayList<>(micro.profile());
        List<BatchingProfiler.AggregatedResult> aggregates =
                new ArrayList<>(BatchingProfiler.aggregate(repetitions));

        BatchingProfiler keyed = BatchingProfiler.keyed(
                LIFECYCLE,
                List.of(keyedExperiment()),
                sequence -> (int) Math.floorMod(sequence, 16),
                ignored -> BatchingProfilerHarness::loadKeys);
        List<BatchingProfiler.Result> keyedRepetitions = keyed.profile();
        repetitions.addAll(keyedRepetitions);
        aggregates.addAll(BatchingProfiler.aggregate(keyedRepetitions));

        List<BatchingProfiler.AggregatedResult> comparable = aggregates.stream()
                .filter(result -> result.experiment().name().startsWith("closed-config"))
                .toList();
        List<BatchingProfiler.AggregatedResult> frontier = ParetoFrontier.efficient(
                comparable,
                List.of(
                        ParetoFrontier.Objective.maximize(
                                BatchingProfiler.AggregatedResult::logicalRequestsPerSecond,
                                ParetoFrontier.Tolerance.relative(0.05)),
                        ParetoFrontier.Objective.minimize(
                                result -> result.latencies().endToEnd().p99(),
                                ParetoFrontier.Tolerance.relative(0.05)),
                        ParetoFrontier.Objective.minimize(result ->
                                result.experiment().config().maxConcurrentBatches())));

        verify(repetitions, aggregates);
        print(aggregates, frontier);
        BatchingProfilerReport.writeCsv(REPORT, repetitions, aggregates, frontier);
        System.out.println("synthetic batching-profiler report: " + REPORT);
    }

    private static List<BatchingProfiler.Experiment> microExperiments() {
        var config64 = new BatchingConfig(
                64, Duration.ofNanos(250_000), 4, 4_096, AdmissionPolicy.WAIT, Duration.ZERO);
        var config128 = new BatchingConfig(
                128, Duration.ofNanos(250_000), 4, 4_096, AdmissionPolicy.WAIT, Duration.ZERO);
        var low = new BatchingConfig(
                128, Duration.ofMillis(1), 4, 1_024, AdmissionPolicy.REJECT, Duration.ZERO);
        var overloadReject = new BatchingConfig(
                64, Duration.ofNanos(250_000), 2, 256, AdmissionPolicy.REJECT, Duration.ZERO);
        var overloadTimeout = new BatchingConfig(
                64,
                Duration.ofNanos(250_000),
                2,
                256,
                AdmissionPolicy.WAIT_WITH_TIMEOUT,
                Duration.ofNanos(100_000));
        var overloadWait = new BatchingConfig(
                64, Duration.ofNanos(250_000), 2, 256, AdmissionPolicy.WAIT, Duration.ZERO);
        return List.of(
                new BatchingProfiler.ClosedLoopExperiment(
                        "closed-config-64", config64, 1_024),
                new BatchingProfiler.ClosedLoopExperiment(
                        "closed-config-128", config128, 1_024),
                new BatchingProfiler.OpenLoopExperiment(
                        "open-low", low, 2_000),
                new BatchingProfiler.OpenLoopExperiment(
                        "open-overload-reject", overloadReject, 80_000),
                new BatchingProfiler.OpenLoopExperiment(
                        "open-overload-timeout", overloadTimeout, 80_000),
                new BatchingProfiler.OpenLoopExperiment(
                        "open-overload-wait", overloadWait, 50_000),
                new BatchingProfiler.ClosedLoopExperiment(
                        "closed-slow-backend", config128, 1_024));
    }

    private static BatchingProfiler.Experiment keyedExperiment() {
        return new BatchingProfiler.ClosedLoopExperiment(
                "keyed-duplicate-heavy",
                new BatchingConfig(
                        128,
                        Duration.ofNanos(250_000),
                        4,
                        4_096,
                        AdmissionPolicy.WAIT,
                        Duration.ZERO),
                1_024);
    }

    private static Duration backendLatency(BatchingProfiler.Experiment experiment) {
        if (experiment.name().contains("overload")) {
            return Duration.ofMillis(4);
        }
        if (experiment.name().contains("slow")) {
            return Duration.ofMillis(6);
        }
        return Duration.ofMillis(1);
    }

    private static Map<Integer, BatchOutcome<Integer>> loadKeys(Set<Integer> keys) {
        delay(Duration.ofMillis(1));
        var results = new HashMap<Integer, BatchOutcome<Integer>>();
        for (int key : keys) {
            if (key % 11 == 0) {
                continue;
            }
            results.put(
                    key,
                    key % 13 == 0
                            ? BatchOutcome.failure(new SyntheticFailure())
                            : BatchOutcome.success(key));
        }
        return results;
    }

    private static void delay(Duration duration) {
        long deadline = System.nanoTime() + duration.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            LockSupport.parkNanos(remaining);
        }
    }

    private static void verify(
            List<BatchingProfiler.Result> repetitions,
            List<BatchingProfiler.AggregatedResult> aggregates) {
        for (BatchingProfiler.Result result : repetitions) {
            if (result.offeredRequests()
                            != result.admittedRequests()
                                    + result.rejectedRequests()
                                    + result.admissionTimeouts()
                    || result.admittedRequests() != result.completedRequests()
                    || result.latencies().endToEnd().count() != result.completedRequests()
                    || result.latencies().preDispatch().count() != result.admittedRequests()
                    || result.latencies().backend().count() != result.completedRequests()
                    || result.latencies().completion().count() != result.completedRequests()
                    || result.maximumPendingRequests()
                            > result.experiment().config().maxPendingRequests()
                    || result.maximumBackendConcurrency()
                            > result.experiment().config().maxConcurrentBatches()
                    || result.backendConcurrencyUtilization() < 0
                    || result.backendConcurrencyUtilization() > 1.01) {
                throw new IllegalStateException("synthetic batching accounting failed for " + result);
            }
        }

        Map<String, BatchingProfiler.AggregatedResult> byName = new HashMap<>();
        for (BatchingProfiler.AggregatedResult result : aggregates) {
            byName.put(result.experiment().name(), result);
            long latencyCount = result.repetitions().stream()
                    .mapToLong(repetition -> repetition.latencies().endToEnd().count())
                    .sum();
            if (result.repetitionCount() != LIFECYCLE.repetitions()
                    || result.latencies().endToEnd().count() != latencyCount
                    || result.stability().minimumLogicalRequestsPerSecond()
                            > result.stability().medianLogicalRequestsPerSecond()
                    || result.stability().medianLogicalRequestsPerSecond()
                            > result.stability().maximumLogicalRequestsPerSecond()) {
                throw new IllegalStateException("synthetic batching aggregation failed for " + result);
            }
        }

        BatchingProfiler.AggregatedResult high = byName.get("closed-config-128");
        BatchingProfiler.AggregatedResult low = byName.get("open-low");
        BatchingProfiler.AggregatedResult reject = byName.get("open-overload-reject");
        BatchingProfiler.AggregatedResult timeout = byName.get("open-overload-timeout");
        BatchingProfiler.AggregatedResult wait = byName.get("open-overload-wait");
        BatchingProfiler.AggregatedResult slow = byName.get("closed-slow-backend");
        BatchingProfiler.AggregatedResult keyed = byName.get("keyed-duplicate-heavy");
        if (high.batchFillRatio() < 0.9
                || high.backendConcurrencyUtilization() < 0.7
                || low.batchFillRatio() > 0.25
                || low.timeTriggeredBatches() <= low.sizeTriggeredBatches()
                || low.openLoopStability().orElseThrow().status()
                        != BatchingProfiler.OpenLoopStatus.STABLE
                || reject.rejectedRequests() == 0
                || reject.openLoopStability().orElseThrow().status()
                        != BatchingProfiler.OpenLoopStatus.BOUNDED_OVERLOAD
                || timeout.admissionTimeouts() == 0
                || wait.rejectedRequests() != 0
                || wait.admissionTimeouts() != 0
                || wait.maximumPendingRequests()
                        != wait.experiment().config().maxPendingRequests()
                || wait.openLoopStability().orElseThrow().status()
                        != BatchingProfiler.OpenLoopStatus.BOUNDED_OVERLOAD
                || slow.latencies().backend().p99() <= high.latencies().backend().p99() * 3
                || slow.latencies().endToEnd().p99() <= high.latencies().endToEnd().p99() * 3
                || keyed.keyedMetrics().orElseThrow().coalescedCallers() == 0
                || keyed.missingRequests() == 0
                || keyed.failedRequests() == 0
                || keyed.keyedMetrics().orElseThrow().logicalRequests()
                        != keyed.dispatchedRequests()) {
            throw new IllegalStateException("synthetic batching sanity checks failed");
        }
    }

    private static void print(
            List<BatchingProfiler.AggregatedResult> results,
            List<BatchingProfiler.AggregatedResult> frontier) {
        var efficient = new HashSet<>(frontier);
        for (BatchingProfiler.AggregatedResult result : results) {
            System.out.printf(
                    Locale.ROOT,
                    "%-24s %8.0f/s batch=%6.1f fill=%5.1f%% util=%5.1f%% e2eP99=%6.2fms backendP99=%6.2fms reject=%6d timeout=%6d stability=%s pareto=%s%n",
                    result.experiment().name(),
                    result.logicalRequestsPerSecond(),
                    result.averageBatchSize(),
                    result.batchFillRatio() * 100,
                    result.backendConcurrencyUtilization() * 100,
                    result.latencies().endToEnd().p99() / 1_000_000d,
                    result.latencies().backend().p99() / 1_000_000d,
                    result.rejectedRequests(),
                    result.admissionTimeouts(),
                    result.openLoopStability()
                            .map(stability -> stability.status().toString())
                            .orElse("n/a"),
                    efficient.contains(result));
        }
    }

    private static final class SyntheticFailure extends RuntimeException {
        private SyntheticFailure() {
            super("synthetic item failure", null, false, false);
        }
    }
}
