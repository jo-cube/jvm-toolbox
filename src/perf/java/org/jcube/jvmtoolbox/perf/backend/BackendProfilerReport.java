package org.jcube.jvmtoolbox.perf.backend;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

public final class BackendProfilerReport {
    private BackendProfilerReport() {}

    public static void writeCsv(
            Path path,
            List<BackendProfiler.Result> repetitions,
            List<BackendProfiler.AggregatedResult> aggregates,
            List<BackendProfiler.AggregatedResult> frontier)
            throws IOException {
        Files.createDirectories(path.getParent());
        Set<BackendProfiler.AggregatedResult> efficient = Set.copyOf(frontier);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write("result_type,batch_size,backend_concurrency,repetition,repetition_count,logical_ops_s,combined_logical_ops_s,min_logical_ops_s,max_logical_ops_s,relative_spread,backend_batches_s,successful,missing,failed,error_rate,elapsed_ns,latency_mean_ns,latency_p50_ns,latency_p95_ns,latency_p99_ns,latency_p999_ns,latency_max_ns,max_observed_concurrency,process_cpu_cores,allocated_bytes,gc_count,gc_millis,start_heap_bytes,end_heap_bytes,pareto");
            writer.newLine();
            for (BackendProfiler.Result repetition : repetitions) {
                writer.write(rawRow(repetition));
                writer.newLine();
            }
            for (BackendProfiler.AggregatedResult aggregate : aggregates) {
                writer.write(aggregateRow(aggregate, efficient.contains(aggregate)));
                writer.newLine();
            }
        }
    }

    private static String rawRow(BackendProfiler.Result result) {
        BackendProfiler.Resources resources = result.resources();
        double throughput = result.logicalOperationsPerSecond();
        return String.join(",", List.of(
                "raw",
                Integer.toString(result.batchSize()),
                Integer.toString(result.backendConcurrency()),
                Integer.toString(result.repetition()),
                "1",
                Double.toString(throughput),
                Double.toString(throughput),
                Double.toString(throughput),
                Double.toString(throughput),
                "0.0",
                Double.toString(result.backendBatchesPerSecond()),
                Long.toString(result.successfulOperations()),
                Long.toString(result.missingOperations()),
                Long.toString(result.failedOperations()),
                Double.toString(result.errorRate()),
                Long.toString(result.elapsedNanos()),
                latency(result.backendLatency()),
                Integer.toString(result.maximumObservedConcurrency()),
                Double.toString(resources.processCpuCores()),
                Long.toString(resources.allocatedBytes()),
                Long.toString(resources.gcCount()),
                Long.toString(resources.gcMillis()),
                Long.toString(resources.startHeapBytes()),
                Long.toString(resources.endHeapBytes()),
                "false"));
    }

    private static String aggregateRow(
            BackendProfiler.AggregatedResult result, boolean efficient) {
        BackendProfiler.Stability stability = result.stability();
        return String.join(",", List.of(
                "aggregate",
                Integer.toString(result.batchSize()),
                Integer.toString(result.backendConcurrency()),
                "",
                Integer.toString(result.repetitionCount()),
                Double.toString(result.logicalOperationsPerSecond()),
                Double.toString(result.combinedLogicalOperationsPerSecond()),
                Double.toString(stability.minimumLogicalOperationsPerSecond()),
                Double.toString(stability.maximumLogicalOperationsPerSecond()),
                Double.toString(stability.relativeSpread()),
                Double.toString(result.backendBatchesPerSecond()),
                Long.toString(result.successfulOperations()),
                Long.toString(result.missingOperations()),
                Long.toString(result.failedOperations()),
                Double.toString(result.errorRate()),
                Long.toString(result.elapsedNanos()),
                latency(result.backendLatency()),
                Integer.toString(result.maximumObservedConcurrency()),
                "",
                "",
                "",
                "",
                "",
                "",
                Boolean.toString(efficient)));
    }

    private static String latency(BackendProfiler.Latency latency) {
        return String.join(",", List.of(
                Double.toString(latency.averageNanos()),
                Long.toString(latency.p50()),
                Long.toString(latency.p95()),
                Long.toString(latency.p99()),
                Long.toString(latency.p999()),
                Long.toString(latency.max())));
    }
}
