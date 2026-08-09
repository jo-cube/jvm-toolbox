package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class CompletionCostBenchmark {
    @State(Scope.Thread)
    public static class CostState {
        private static final List<BatchOutcome<Integer>> INTEGER_RESULT =
                List.of(BatchOutcome.success(BenchmarkSupport.VALUE));
        private static final List<BatchOutcome<Optional<Integer>>> OPTIONAL_RESULT =
                List.of(BatchOutcome.success(Optional.of(BenchmarkSupport.VALUE)));
        private static final Map<Integer, BatchOutcome<Integer>> KEY_RESULT =
                Map.of(BenchmarkSupport.VALUE, BatchOutcome.success(BenchmarkSupport.VALUE));

        MicroBatcher<Integer, Integer> batcher;
        MicroBatcher<Integer, Optional<Integer>> optionalBatcher;
        KeyBatchLoader<Integer, Integer> loader;
        int value;

        @Setup(Level.Trial)
        public void setUp() {
            var config = BenchmarkSupport.config(
                    1, Duration.ZERO, 1, 1_024, AdmissionPolicy.WAIT, Duration.ZERO);
            batcher = new MicroBatcher<>(config, ignored -> INTEGER_RESULT);
            optionalBatcher = new MicroBatcher<>(config, ignored -> OPTIONAL_RESULT);
            loader = new KeyBatchLoader<>(config, ignored -> KEY_RESULT);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            batcher.close();
            optionalBatcher.close();
            loader.close();
        }

        int nextValue() {
            return ++value;
        }
    }

    @Benchmark
    public int directInvocation(CostState state) {
        return state.nextValue();
    }

    @Benchmark
    public int directCompletableFuture(CostState state, Blackhole blackhole) {
        var future = CompletableFuture.completedFuture(state.nextValue());
        blackhole.consume(future);
        return future.join();
    }

    @Benchmark
    public int directOptional(CostState state, Blackhole blackhole) {
        var optional = Optional.of(state.nextValue());
        blackhole.consume(optional);
        return optional.orElseThrow();
    }

    @Benchmark
    public int directOptionalCompletableFuture(CostState state, Blackhole blackhole) {
        var future = CompletableFuture.completedFuture(Optional.of(state.nextValue()));
        blackhole.consume(future);
        return future.join().orElseThrow();
    }

    @Benchmark
    public int microBatcher(CostState state) throws Exception {
        return state.batcher.submit(BenchmarkSupport.VALUE).join();
    }

    @Benchmark
    public Optional<Integer> microBatcherOptional(CostState state) throws Exception {
        return state.optionalBatcher.submit(BenchmarkSupport.VALUE).join();
    }

    @Benchmark
    public Optional<Integer> keyBatchLoader(CostState state) throws Exception {
        return state.loader.load(BenchmarkSupport.VALUE).join();
    }
}
