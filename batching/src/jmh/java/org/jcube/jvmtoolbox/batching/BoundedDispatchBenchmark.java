package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
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
public class BoundedDispatchBenchmark {
    private static final List<BatchOutcome<Integer>> RESULT =
            List.of(BatchOutcome.success(BenchmarkSupport.VALUE));

    @Param({"1", "2", "4"})
    public int maxConcurrentBatches;

    private MicroBatcher<Integer, Integer> batcher;
    private CompletableFuture<Integer>[] futures;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() {
        var barrier = new CyclicBarrier(maxConcurrentBatches);
        var config = BenchmarkSupport.config(
                1,
                Duration.ZERO,
                maxConcurrentBatches,
                maxConcurrentBatches,
                AdmissionPolicy.WAIT,
                Duration.ZERO);
        futures = (CompletableFuture<Integer>[]) new CompletableFuture<?>[maxConcurrentBatches];
        batcher = new MicroBatcher<>(config, ignored -> {
            barrier.await();
            return RESULT;
        });
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        batcher.close();
    }

    @Benchmark
    public int dispatchAtLimit() throws Exception {
        for (int index = 0; index < maxConcurrentBatches; index++) {
            futures[index] = batcher.submit(index);
        }
        int sum = 0;
        for (var future : futures) {
            sum += future.join();
        }
        return sum;
    }
}
