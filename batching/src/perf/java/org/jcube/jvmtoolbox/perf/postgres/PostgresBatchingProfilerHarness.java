package org.jcube.jvmtoolbox.perf.postgres;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.jcube.jvmtoolbox.batching.AdmissionPolicy;
import org.jcube.jvmtoolbox.batching.BatchOutcome;
import org.jcube.jvmtoolbox.batching.BatchingConfig;
import org.jcube.jvmtoolbox.perf.BatchingProfiler;
import org.jcube.jvmtoolbox.perf.BatchingProfilerReport;
import org.jcube.jvmtoolbox.perf.backend.ParetoFrontier;

public final class PostgresBatchingProfilerHarness {
    private static final Path REPORT =
            Path.of("build", "reports", "perf", "postgres-batching-profiler.csv");

    private PostgresBatchingProfilerHarness() {}

    public static void main(String[] args) throws Exception {
        PostgresSettings settings = PostgresSettings.environment();
        DatasetInfo dataset = JdbcLookupBackend.datasetInfo(settings);
        if (dataset.rows() == 0) {
            throw new IllegalStateException("lookup_value is empty; run just postgres-seed first");
        }
        Files.createDirectories(REPORT.getParent());
        System.out.printf(
                Locale.ROOT,
                "PostgreSQL batching-profiler dataset: %,d rows, %.1f MiB%n",
                dataset.rows(),
                dataset.relationBytes() / 1_048_576d);

        List<BatchingProfiler.Experiment> experiments = experiments();
        BatchingProfiler profiler = BatchingProfiler.keyed(
                new BatchingProfiler.Lifecycle(
                        Duration.ofMillis(250), Duration.ofMillis(750), 3),
                experiments,
                sequence -> presentKey(sequence, dataset.rows()),
                experiment -> backend(settings, experiment));
        List<BatchingProfiler.Result> repetitions = profiler.profile();
        List<BatchingProfiler.AggregatedResult> aggregates =
                BatchingProfiler.aggregate(repetitions);

        List<BatchingProfiler.AggregatedResult> closed = aggregates.stream()
                .filter(result -> result.experiment()
                        instanceof BatchingProfiler.ClosedLoopExperiment)
                .toList();
        List<BatchingProfiler.AggregatedResult> open = aggregates.stream()
                .filter(result -> result.experiment()
                        instanceof BatchingProfiler.OpenLoopExperiment)
                .toList();
        List<BatchingProfiler.AggregatedResult> closedFrontier = frontier(closed);
        List<BatchingProfiler.AggregatedResult> openFrontier = frontier(open);
        var reportFrontier = new ArrayList<BatchingProfiler.AggregatedResult>(closedFrontier);
        reportFrontier.addAll(openFrontier);

        verify(repetitions, aggregates);
        print(aggregates, reportFrontier);
        System.out.println("PostgreSQL closed-loop frontier: " + names(closedFrontier));
        System.out.println("PostgreSQL open-loop frontier: " + names(openFrontier));
        BatchingProfilerReport.writeCsv(REPORT, repetitions, aggregates, reportFrontier);
        System.out.println("PostgreSQL batching-profiler report: " + REPORT);
    }

    private static List<BatchingProfiler.Experiment> experiments() {
        var config128x8 = new BatchingConfig(
                128, Duration.ofNanos(500_000), 8, 16_384, AdmissionPolicy.WAIT, Duration.ZERO);
        var config512x4 = new BatchingConfig(
                512, Duration.ofNanos(500_000), 4, 16_384, AdmissionPolicy.WAIT, Duration.ZERO);
        var open128x8 = new BatchingConfig(
                128, Duration.ofNanos(500_000), 8, 16_384, AdmissionPolicy.REJECT, Duration.ZERO);
        return List.of(
                new BatchingProfiler.ClosedLoopExperiment(
                        "postgres-closed-128x8", config128x8, 4_096),
                new BatchingProfiler.ClosedLoopExperiment(
                        "postgres-closed-512x4", config512x4, 4_096),
                new BatchingProfiler.OpenLoopExperiment(
                        "postgres-open-128x8", open128x8, 250_000));
    }

    private static BatchingProfiler.KeyBackend<LookupKey, BigDecimal> backend(
            PostgresSettings settings, BatchingProfiler.Experiment experiment)
            throws Exception {
        var jdbc = new JdbcLookupBackend(
                settings, experiment.config().maxConcurrentBatches());
        return new BatchingProfiler.KeyBackend<>() {
            @Override
            public Map<LookupKey, BatchOutcome<BigDecimal>> load(Set<LookupKey> keys)
                    throws Exception {
                return jdbc.load(keys);
            }

            @Override
            public void close() {
                jdbc.close();
            }
        };
    }

    private static LookupKey presentKey(long sequence, long rows) {
        long ordinal = Math.floorMod(sequence, rows);
        return new LookupKey("tenant-" + ordinal / 1_000, (int) (ordinal % 1_000));
    }

    private static List<BatchingProfiler.AggregatedResult> frontier(
            List<BatchingProfiler.AggregatedResult> results) {
        return ParetoFrontier.efficient(
                results,
                List.of(
                        ParetoFrontier.Objective.maximize(
                                BatchingProfiler.AggregatedResult::logicalRequestsPerSecond,
                                ParetoFrontier.Tolerance.relative(0.05)),
                        ParetoFrontier.Objective.minimize(
                                result -> result.latencies().endToEnd().p99(),
                                ParetoFrontier.Tolerance.relative(0.05)),
                        ParetoFrontier.Objective.minimize(result ->
                                result.experiment().config().maxConcurrentBatches())));
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
                    || result.failedRequests() != 0
                    || result.missingRequests() != 0
                    || result.maximumPendingRequests()
                            > result.experiment().config().maxPendingRequests()
                    || result.maximumBackendConcurrency()
                            > result.experiment().config().maxConcurrentBatches()
                    || result.latencies().endToEnd().count() != result.completedRequests()
                    || result.keyedMetrics().orElseThrow().logicalRequests()
                            != result.dispatchedRequests()) {
                throw new IllegalStateException("PostgreSQL batching-profiler invariant failed");
            }
        }
        if (aggregates.size() != experiments().size()
                || aggregates.stream().anyMatch(result -> result.repetitionCount() != 3)) {
            throw new IllegalStateException("PostgreSQL batching aggregation failed");
        }
    }

    private static void print(
            List<BatchingProfiler.AggregatedResult> results,
            List<BatchingProfiler.AggregatedResult> frontier) {
        var efficient = new HashSet<>(frontier);
        for (BatchingProfiler.AggregatedResult result : results) {
            System.out.printf(
                    Locale.ROOT,
                    "%-28s logical=%9.0f/s batch=%6.1f fill=%5.1f%% util=%5.1f%% preP99=%6.2fms backendP99=%6.2fms completionP99=%6.2fms e2eP99=%6.2fms reject=%6d stability=%s pareto=%s%n",
                    result.experiment().name(),
                    result.logicalRequestsPerSecond(),
                    result.averageBatchSize(),
                    result.batchFillRatio() * 100,
                    result.backendConcurrencyUtilization() * 100,
                    result.latencies().preDispatch().p99() / 1_000_000d,
                    result.latencies().backend().p99() / 1_000_000d,
                    result.latencies().completion().p99() / 1_000_000d,
                    result.latencies().endToEnd().p99() / 1_000_000d,
                    result.rejectedRequests(),
                    result.openLoopStability()
                            .map(stability -> stability.status().toString())
                            .orElse("n/a"),
                    efficient.contains(result));
        }
    }

    private static List<String> names(List<BatchingProfiler.AggregatedResult> results) {
        return results.stream().map(result -> result.experiment().name()).toList();
    }
}
