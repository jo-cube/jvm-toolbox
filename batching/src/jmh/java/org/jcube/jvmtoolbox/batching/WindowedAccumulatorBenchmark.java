package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WindowedAccumulatorBenchmark {
    @State(Scope.Group)
    public static class AccumulatorState {
        private final LongAdder processed = new LongAdder();
        private WindowedAccumulator<Integer, long[]> accumulator;

        @Setup(Level.Trial)
        public void setUp() {
            var config = new WindowedAccumulatorConfig(
                    128,
                    Duration.ofNanos(50_000),
                    4_096,
                    AdmissionPolicy.WAIT,
                    Duration.ZERO);
            accumulator = new WindowedAccumulator<>(
                    config,
                    () -> new long[1],
                    (sum, input) -> sum[0] += input,
                    sum -> processed.add(sum[0]));
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            accumulator.close();
            if (processed.sum() == 0) {
                throw new IllegalStateException("benchmark processed no contributions");
            }
        }

        void add() throws Exception {
            accumulator.add(BenchmarkSupport.VALUE);
        }
    }

    @Benchmark
    @Group("accumulatorProducers01")
    @GroupThreads(1)
    public void producers01(AccumulatorState state) throws Exception {
        state.add();
    }

    @Benchmark
    @Group("accumulatorProducers02")
    @GroupThreads(2)
    public void producers02(AccumulatorState state) throws Exception {
        state.add();
    }

    @Benchmark
    @Group("accumulatorProducers04")
    @GroupThreads(4)
    public void producers04(AccumulatorState state) throws Exception {
        state.add();
    }

    @Benchmark
    @Group("accumulatorProducers08")
    @GroupThreads(8)
    public void producers08(AccumulatorState state) throws Exception {
        state.add();
    }

    @Benchmark
    @Group("accumulatorProducers16")
    @GroupThreads(16)
    public void producers16(AccumulatorState state) throws Exception {
        state.add();
    }
}
