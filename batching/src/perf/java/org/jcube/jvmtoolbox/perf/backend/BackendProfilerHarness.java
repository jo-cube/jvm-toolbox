package org.jcube.jvmtoolbox.perf.backend;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.LockSupport;

public final class BackendProfilerHarness {
    private static final Path REPORT =
            Path.of("build", "reports", "perf", "backend-synthetic-results.csv");

    private BackendProfilerHarness() {}

    public static void main(String[] args) throws Exception {
        var config = new BackendProfiler.Config(
                List.of(16, 64, 256),
                List.of(1, 4, 8),
                Duration.ofMillis(100),
                Duration.ofMillis(300),
                3);
        var profiler = new BackendProfiler<Long>(
                config,
                BackendProfilerHarness::inputs,
                concurrency -> batch -> process(batch));

        List<BackendProfiler.Result> repetitions = profiler.profile();
        List<BackendProfiler.AggregatedResult> aggregates =
                BackendProfiler.aggregate(repetitions);
        List<BackendProfiler.AggregatedResult> frontier = ParetoFrontier.efficient(
                aggregates,
                List.of(
                        ParetoFrontier.Objective.maximize(
                                BackendProfiler.AggregatedResult::logicalOperationsPerSecond,
                                ParetoFrontier.Tolerance.relative(0.02)),
                        ParetoFrontier.Objective.minimize(
                                result -> result.backendLatency().p99(),
                                ParetoFrontier.Tolerance.relative(0.05)),
                        ParetoFrontier.Objective.minimize(
                                BackendProfiler.AggregatedResult::backendConcurrency)));
        verifyParetoSemantics();
        verify(repetitions, aggregates, frontier, config);
        print(aggregates, frontier);
        write(repetitions, aggregates, frontier);
    }

    private static List<Long> inputs(int batchSize, long batchSequence) {
        long base = batchSequence * batchSize;
        var inputs = new ArrayList<Long>(batchSize);
        for (int index = 0; index < batchSize; index++) {
            inputs.add(base + index);
        }
        return inputs;
    }

    private static BackendProfiler.BatchResult process(List<Long> batch) {
        LockSupport.parkNanos(180_000L + batch.size() * 1_200L);
        int successful = 0;
        int missing = 0;
        int failed = 0;
        for (long input : batch) {
            if (input % 37 == 0) {
                failed++;
            } else if (input % 10 == 0) {
                missing++;
            } else {
                successful++;
            }
        }
        return new BackendProfiler.BatchResult(successful, missing, failed);
    }

    private static void verify(
            List<BackendProfiler.Result> repetitions,
            List<BackendProfiler.AggregatedResult> aggregates,
            List<BackendProfiler.AggregatedResult> frontier,
            BackendProfiler.Config config) {
        int expectedResults = config.batchSizes().size()
                * config.backendConcurrencies().size()
                * config.repetitions();
        int expectedAggregates = config.batchSizes().size()
                * config.backendConcurrencies().size();
        if (repetitions.size() != expectedResults
                || aggregates.size() != expectedAggregates
                || frontier.size() < 2) {
            throw new IllegalStateException("synthetic profiler did not produce a useful Pareto frontier");
        }
        for (BackendProfiler.Result result : repetitions) {
            if (result.logicalOperations() == 0
                    || result.missingOperations() == 0
                    || result.failedOperations() == 0
                    || result.backendBatches() != result.backendLatency().count()
                    || result.maximumObservedConcurrency() > result.backendConcurrency()) {
                throw new IllegalStateException("synthetic profiler invariants failed for " + result);
            }
        }
        for (BackendProfiler.AggregatedResult result : aggregates) {
            long logicalOperations = result.repetitions().stream()
                    .mapToLong(BackendProfiler.Result::logicalOperations)
                    .sum();
            long elapsed = result.repetitions().stream()
                    .mapToLong(BackendProfiler.Result::elapsedNanos)
                    .sum();
            long latencyCount = result.repetitions().stream()
                    .mapToLong(repetition -> repetition.backendLatency().count())
                    .sum();
            long latencyMax = result.repetitions().stream()
                    .mapToLong(repetition -> repetition.backendLatency().max())
                    .max()
                    .orElseThrow();
            double median = result.repetitions().stream()
                    .mapToDouble(BackendProfiler.Result::logicalOperationsPerSecond)
                    .sorted()
                    .toArray()[1];
            BackendProfiler.Stability stability = result.stability();
            if (result.repetitionCount() != config.repetitions()
                    || result.logicalOperations() != logicalOperations
                    || result.elapsedNanos() != elapsed
                    || result.backendLatency().count() != latencyCount
                    || result.backendLatency().max() != latencyMax
                    || result.backendBatches() != latencyCount
                    || result.logicalOperationsPerSecond() != median
                    || stability.minimumLogicalOperationsPerSecond()
                            > stability.medianLogicalOperationsPerSecond()
                    || stability.medianLogicalOperationsPerSecond()
                            > stability.maximumLogicalOperationsPerSecond()
                    || stability.relativeSpread() < 0
                    || result.maximumObservedConcurrency() > result.backendConcurrency()) {
                throw new IllegalStateException("synthetic aggregation invariants failed for " + result);
            }
        }
    }

    private static void verifyParetoSemantics() {
        var fast = new Candidate(100, 10, 8);
        var lean = new Candidate(80, 2, 1);
        var dominated = new Candidate(70, 12, 8);
        List<Candidate> frontier = ParetoFrontier.efficient(
                List.of(fast, lean, dominated),
                List.of(
                        ParetoFrontier.Objective.maximize(Candidate::throughput),
                        ParetoFrontier.Objective.minimize(Candidate::latency),
                        ParetoFrontier.Objective.minimize(Candidate::concurrency)));
        if (!frontier.equals(List.of(fast, lean))) {
            throw new IllegalStateException("Pareto dominance semantics failed: " + frontier);
        }

        var slightlyFaster = new Candidate(101, 10, 8);
        var efficientLean = new Candidate(100, 5, 4);
        List<Candidate> exact = ParetoFrontier.efficient(
                List.of(slightlyFaster, efficientLean),
                List.of(
                        ParetoFrontier.Objective.maximize(Candidate::throughput),
                        ParetoFrontier.Objective.minimize(Candidate::latency),
                        ParetoFrontier.Objective.minimize(Candidate::concurrency)));
        List<Candidate> relative = ParetoFrontier.efficient(
                List.of(slightlyFaster, efficientLean),
                List.of(
                        ParetoFrontier.Objective.maximize(
                                Candidate::throughput,
                                ParetoFrontier.Tolerance.relative(0.02)),
                        ParetoFrontier.Objective.minimize(Candidate::latency),
                        ParetoFrontier.Objective.minimize(Candidate::concurrency)));
        List<Candidate> absolute = ParetoFrontier.efficient(
                List.of(slightlyFaster, efficientLean),
                List.of(
                        ParetoFrontier.Objective.maximize(
                                Candidate::throughput,
                                ParetoFrontier.Tolerance.absolute(2)),
                        ParetoFrontier.Objective.minimize(Candidate::latency),
                        ParetoFrontier.Objective.minimize(Candidate::concurrency)));
        if (!exact.equals(List.of(slightlyFaster, efficientLean))
                || !relative.equals(List.of(efficientLean))
                || !absolute.equals(List.of(efficientLean))) {
            throw new IllegalStateException("Pareto tolerance semantics failed");
        }
    }

    private static void print(
            List<BackendProfiler.AggregatedResult> results,
            List<BackendProfiler.AggregatedResult> frontier) {
        var efficient = new HashSet<>(frontier);
        for (BackendProfiler.AggregatedResult result : results) {
            System.out.printf(
                    Locale.ROOT,
                    "synthetic batch=%3d concurrency=%d logical=%9.0f/s spread=%4.1f%% p99=%6.3fms errors=%4.1f%% pareto=%s%n",
                    result.batchSize(),
                    result.backendConcurrency(),
                    result.logicalOperationsPerSecond(),
                    result.stability().relativeSpread() * 100,
                    result.backendLatency().p99() / 1_000_000d,
                    result.errorRate() * 100,
                    efficient.contains(result));
        }
        System.out.println("synthetic Pareto-efficient configurations: " + frontier.size());
    }

    private static void write(
            List<BackendProfiler.Result> repetitions,
            List<BackendProfiler.AggregatedResult> aggregates,
            List<BackendProfiler.AggregatedResult> frontier)
            throws IOException {
        BackendProfilerReport.writeCsv(REPORT, repetitions, aggregates, frontier);
        System.out.println("synthetic profiler report: " + REPORT);
    }

    private record Candidate(double throughput, double latency, double concurrency) {}
}
