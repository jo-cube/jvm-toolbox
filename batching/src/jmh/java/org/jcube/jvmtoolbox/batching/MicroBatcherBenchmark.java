package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Thread)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MicroBatcherBenchmark {
    @Param({"1", "8", "32", "128"})
    public int batchSize;

    @Param({"false", "true"})
    public boolean statisticsEnabled;

    private MicroBatcher<Integer, Integer> batcher;
    private BatchStatistics statistics;
    private CompletableFuture<Integer>[] futures;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() {
        List<BatchOutcome<Integer>> outcomes = BenchmarkSupport.integerOutcomes(batchSize);
        var config = BenchmarkSupport.config(
                batchSize,
                Duration.ofSeconds(5),
                1,
                batchSize * 2,
                AdmissionPolicy.WAIT,
                Duration.ZERO);
        futures = (CompletableFuture<Integer>[]) new CompletableFuture<?>[batchSize];
        if (statisticsEnabled) {
            statistics = new BatchStatistics();
            batcher = new MicroBatcher<>(config, ignored -> outcomes, statistics);
        } else {
            batcher = new MicroBatcher<>(config, ignored -> outcomes);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        batcher.close();
        if (statistics != null) {
            var snapshot = statistics.snapshot();
            if (snapshot.incompleteRequests() != 0
                    || snapshot.admittedRequests() == 0
                    || snapshot.admittedRequests() != snapshot.dispatchedRequests()) {
                throw new IllegalStateException("observed benchmark counters do not match its work");
            }
        }
    }

    @Benchmark
    public int batch() throws Exception {
        for (int index = 0; index < batchSize; index++) {
            futures[index] = batcher.submit(index);
        }
        int sum = 0;
        for (var future : futures) {
            sum += future.join();
        }
        return sum;
    }
}
