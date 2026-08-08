package org.jcube.jvmtoolbox.perf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class BatchingProfilerReport {
    private BatchingProfilerReport() {}

    public static void writeCsv(
            Path path,
            List<BatchingProfiler.Result> repetitions,
            List<BatchingProfiler.AggregatedResult> aggregates,
            List<BatchingProfiler.AggregatedResult> frontier)
            throws IOException {
        Files.createDirectories(path.getParent());
        Set<BatchingProfiler.AggregatedResult> efficient = Set.copyOf(frontier);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(header());
            writer.newLine();
            for (BatchingProfiler.Result repetition : repetitions) {
                writer.write(rawRow(repetition));
                writer.newLine();
            }
            for (BatchingProfiler.AggregatedResult aggregate : aggregates) {
                writer.write(aggregateRow(aggregate, efficient.contains(aggregate)));
                writer.newLine();
            }
        }
    }

    private static String rawRow(BatchingProfiler.Result result) {
        var row = startRow("raw", result.target(), result.experiment());
        double throughput = result.logicalRequestsPerSecond();
        row.add(Integer.toString(result.repetition()));
        row.add("1");
        row.add(Double.toString(throughput));
        row.add(Double.toString(throughput));
        row.add(Double.toString(throughput));
        row.add(Double.toString(throughput));
        row.add("0.0");
        row.add(Long.toString(result.elapsedNanos()));
        appendMetrics(
                row,
                result.offeredRequests(),
                result.admittedRequests(),
                result.completedRequests(),
                result.rejectedRequests(),
                result.admissionTimeouts(),
                result.failedRequests(),
                result.successfulRequests(),
                result.missingRequests(),
                result.backendBatchesPerSecond(),
                result.backendBatches(),
                result.averageBatchSize(),
                result.batchFillRatio(),
                result.sizeTriggeredBatches(),
                result.timeTriggeredBatches(),
                result.maximumPendingRequests(),
                result.maximumBackendConcurrency(),
                result.backendConcurrencyUtilization(),
                result.keyedMetrics(),
                result.openLoopStability(),
                result.latencies());
        BatchingProfiler.Resources resources = result.resources();
        row.add(Double.toString(resources.processCpuCores()));
        row.add(Long.toString(resources.allocatedBytes()));
        row.add(Long.toString(resources.gcCount()));
        row.add(Long.toString(resources.gcMillis()));
        row.add(Long.toString(resources.startHeapBytes()));
        row.add(Long.toString(resources.endHeapBytes()));
        row.add("false");
        return String.join(",", row);
    }

    private static String aggregateRow(
            BatchingProfiler.AggregatedResult result, boolean efficient) {
        var row = startRow("aggregate", result.target(), result.experiment());
        BatchingProfiler.Stability stability = result.stability();
        row.add("");
        row.add(Integer.toString(result.repetitionCount()));
        row.add(Double.toString(result.logicalRequestsPerSecond()));
        row.add(Double.toString(result.combinedLogicalRequestsPerSecond()));
        row.add(Double.toString(stability.minimumLogicalRequestsPerSecond()));
        row.add(Double.toString(stability.maximumLogicalRequestsPerSecond()));
        row.add(Double.toString(stability.relativeSpread()));
        row.add(Long.toString(result.elapsedNanos()));
        appendMetrics(
                row,
                result.offeredRequests(),
                result.admittedRequests(),
                result.completedRequests(),
                result.rejectedRequests(),
                result.admissionTimeouts(),
                result.failedRequests(),
                result.successfulRequests(),
                result.missingRequests(),
                result.backendBatchesPerSecond(),
                result.backendBatches(),
                result.averageBatchSize(),
                result.batchFillRatio(),
                result.sizeTriggeredBatches(),
                result.timeTriggeredBatches(),
                result.maximumPendingRequests(),
                result.maximumBackendConcurrency(),
                result.backendConcurrencyUtilization(),
                result.keyedMetrics(),
                result.openLoopStability(),
                result.latencies());
        row.add("");
        row.add("");
        row.add("");
        row.add("");
        row.add("");
        row.add("");
        row.add(Boolean.toString(efficient));
        return String.join(",", row);
    }

    private static ArrayList<String> startRow(
            String type, BatchingProfiler.Target target, BatchingProfiler.Experiment experiment) {
        var row = new ArrayList<String>();
        BatchingProfiler.BatchingConfiguration batching = experiment.batching();
        row.add(type);
        row.add(target.toString());
        row.add(experiment instanceof BatchingProfiler.ClosedLoopExperiment ? "closed" : "open");
        row.add(experiment.name());
        row.add(Integer.toString(batching.maxBatchSize()));
        row.add(Long.toString(batching.maxWait().toNanos()));
        row.add(Integer.toString(batching.maxConcurrentBatches()));
        row.add(Integer.toString(batching.maxPendingRequests()));
        row.add(experiment.admission().policy().toString());
        row.add(Long.toString(experiment.admission().timeout().toNanos()));
        row.add(experiment instanceof BatchingProfiler.ClosedLoopExperiment closed
                ? Integer.toString(closed.foregroundConcurrency())
                : "");
        row.add(experiment instanceof BatchingProfiler.OpenLoopExperiment open
                ? Long.toString(open.offeredRequestsPerSecond())
                : "");
        return row;
    }

    private static void appendMetrics(
            List<String> row,
            long offered,
            long admitted,
            long completed,
            long rejected,
            long timeouts,
            long failed,
            long successful,
            long missing,
            double batchesPerSecond,
            long batches,
            double averageBatch,
            double fill,
            long sizeTriggered,
            long timeTriggered,
            long maxPending,
            int maxBackendConcurrency,
            double backendUtilization,
            java.util.Optional<BatchingProfiler.KeyedMetrics> keyed,
            java.util.Optional<BatchingProfiler.OpenLoopStability> open,
            BatchingProfiler.Latencies latencies) {
        row.add(Long.toString(offered));
        row.add(Long.toString(admitted));
        row.add(Long.toString(completed));
        row.add(Long.toString(rejected));
        row.add(Long.toString(timeouts));
        row.add(Long.toString(failed));
        row.add(Long.toString(successful));
        row.add(Long.toString(missing));
        row.add(Double.toString(batchesPerSecond));
        row.add(Long.toString(batches));
        row.add(Double.toString(averageBatch));
        row.add(Double.toString(fill));
        row.add(Long.toString(sizeTriggered));
        row.add(Long.toString(timeTriggered));
        row.add(Long.toString(maxPending));
        row.add(Integer.toString(maxBackendConcurrency));
        row.add(Double.toString(backendUtilization));
        if (keyed.isPresent()) {
            BatchingProfiler.KeyedMetrics metrics = keyed.orElseThrow();
            row.add(Long.toString(metrics.logicalRequests()));
            row.add(Long.toString(metrics.uniqueBackendKeys()));
            row.add(Long.toString(metrics.coalescedCallers()));
            row.add(Double.toString(metrics.coalescingRatio()));
        } else {
            row.add("");
            row.add("");
            row.add("");
            row.add("");
        }
        if (open.isPresent()) {
            BatchingProfiler.OpenLoopStability stability = open.orElseThrow();
            row.add(stability.status().toString());
            row.add(Long.toString(stability.midpointOutstanding()));
            row.add(Long.toString(stability.offerEndOutstanding()));
            row.add(Long.toString(stability.outstandingGrowth()));
            row.add(Long.toString(stability.allowedGrowth()));
        } else {
            row.add("");
            row.add("");
            row.add("");
            row.add("");
            row.add("");
        }
        append(row, latencies.endToEnd());
        append(row, latencies.preDispatch());
        append(row, latencies.backend());
        append(row, latencies.completion());
    }

    private static void append(List<String> row, BatchingProfiler.Distribution latency) {
        row.add(Long.toString(latency.count()));
        row.add(Double.toString(latency.averageNanos()));
        row.add(Long.toString(latency.p50()));
        row.add(Long.toString(latency.p95()));
        row.add(Long.toString(latency.p99()));
        row.add(Long.toString(latency.p999()));
        row.add(Long.toString(latency.max()));
    }

    private static String header() {
        var columns = new ArrayList<>(List.of(
                "result_type",
                "target",
                "experiment_type",
                "experiment",
                "max_batch_size",
                "max_wait_ns",
                "max_concurrent_batches",
                "max_pending_requests",
                "admission_policy",
                "admission_timeout_ns",
                "foreground_concurrency",
                "offered_rate",
                "repetition",
                "repetition_count",
                "logical_req_s",
                "combined_logical_req_s",
                "min_logical_req_s",
                "max_logical_req_s",
                "relative_spread",
                "elapsed_ns",
                "offered",
                "admitted",
                "completed",
                "rejected",
                "timeouts",
                "failed",
                "successful",
                "missing",
                "backend_batches_s",
                "backend_batches",
                "average_batch_size",
                "batch_fill_ratio",
                "size_triggered_batches",
                "time_triggered_batches",
                "observed_max_pending_requests",
                "max_backend_concurrency",
                "backend_concurrency_utilization",
                "keyed_logical_requests",
                "unique_backend_keys",
                "coalesced_callers",
                "coalescing_ratio",
                "open_loop_status",
                "midpoint_outstanding",
                "offer_end_outstanding",
                "outstanding_growth",
                "allowed_growth"));
        for (String prefix : List.of("end_to_end", "pre_dispatch", "backend", "completion")) {
            for (String suffix : List.of("count", "mean_ns", "p50_ns", "p95_ns", "p99_ns", "p999_ns", "max_ns")) {
                columns.add(prefix + "_" + suffix);
            }
        }
        columns.addAll(List.of(
                "process_cpu_cores",
                "allocated_bytes",
                "gc_count",
                "gc_millis",
                "start_heap_bytes",
                "end_heap_bytes",
                "pareto"));
        return String.join(",", columns);
    }
}
