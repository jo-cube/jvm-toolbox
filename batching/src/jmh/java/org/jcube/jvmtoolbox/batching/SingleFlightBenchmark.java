package org.jcube.jvmtoolbox.batching;

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
import org.openjdk.jmh.annotations.Threads;

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SingleFlightBenchmark {
    @State(Scope.Thread)
    public static class CompletedState {
        SingleFlight<Integer, Integer> singleFlight;

        @Setup(Level.Trial)
        public void setUp() {
            singleFlight = new SingleFlight<>(CompletableFuture::completedFuture);
        }
    }

    @State(Scope.Benchmark)
    public static class ContendedState {
        CompletableFuture<Integer> operation;
        SingleFlight<Integer, Integer> singleFlight;

        @Setup(Level.Trial)
        public void setUp() {
            operation = new CompletableFuture<>();
            singleFlight = new SingleFlight<>(ignored -> operation);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            operation.complete(BenchmarkSupport.VALUE);
        }
    }

    @Benchmark
    public int completedOperation(CompletedState state) {
        return state.singleFlight.execute(BenchmarkSupport.VALUE).join();
    }

    @Benchmark
    @Threads(Threads.MAX)
    public CompletableFuture<Integer> contendedWaiter(ContendedState state) {
        return state.singleFlight.execute(BenchmarkSupport.VALUE);
    }
}
