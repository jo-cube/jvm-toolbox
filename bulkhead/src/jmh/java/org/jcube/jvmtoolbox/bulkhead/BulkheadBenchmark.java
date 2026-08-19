package org.jcube.jvmtoolbox.bulkhead;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class BulkheadBenchmark {
    @State(Scope.Thread)
    public static class CallState {
        private final Bulkhead bulkhead = new Bulkhead(1, 0);
        private final Callable<Integer> operation = this::next;
        private int value;

        private int next() {
            return ++value;
        }
    }

    @Benchmark
    public int direct(CallState state) {
        return state.next();
    }

    @Benchmark
    public int bulkhead(CallState state) throws Exception {
        return state.bulkhead.call(state.operation);
    }
}
