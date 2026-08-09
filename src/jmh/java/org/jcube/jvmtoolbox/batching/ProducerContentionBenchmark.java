package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
public class ProducerContentionBenchmark {
    @State(Scope.Group)
    public static class ContendedState {
        private List<List<BatchOutcome<Integer>>> outcomesBySize;
        private MicroBatcher<Integer, Integer> batcher;

        @Setup(Level.Trial)
        public void setUp() {
            outcomesBySize = new ArrayList<>(9);
            outcomesBySize.add(List.of());
            for (int size = 1; size <= 8; size++) {
                outcomesBySize.add(BenchmarkSupport.integerOutcomes(size));
            }
            var config = BenchmarkSupport.config(
                    8,
                    Duration.ofNanos(50_000),
                    4,
                    4_096,
                    AdmissionPolicy.WAIT,
                    Duration.ZERO);
            batcher = new MicroBatcher<>(config, inputs -> outcomesBySize.get(inputs.size()));
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            batcher.close();
        }

        int submitAndJoin() throws Exception {
            return batcher.submit(BenchmarkSupport.VALUE).join();
        }
    }

    @Benchmark
    @Group("producers01")
    @GroupThreads(1)
    public int producers01(ContendedState state) throws Exception {
        return state.submitAndJoin();
    }

    @Benchmark
    @Group("producers02")
    @GroupThreads(2)
    public int producers02(ContendedState state) throws Exception {
        return state.submitAndJoin();
    }

    @Benchmark
    @Group("producers04")
    @GroupThreads(4)
    public int producers04(ContendedState state) throws Exception {
        return state.submitAndJoin();
    }

    @Benchmark
    @Group("producers08")
    @GroupThreads(8)
    public int producers08(ContendedState state) throws Exception {
        return state.submitAndJoin();
    }

    @Benchmark
    @Group("producers16")
    @GroupThreads(16)
    public int producers16(ContendedState state) throws Exception {
        return state.submitAndJoin();
    }
}
