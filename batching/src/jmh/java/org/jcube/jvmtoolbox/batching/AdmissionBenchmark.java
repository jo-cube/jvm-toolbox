package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AdmissionBenchmark {
    private static final java.util.List<BatchOutcome<Integer>> RESULT =
            java.util.List.of(BatchOutcome.success(BenchmarkSupport.VALUE));

    @State(Scope.Thread)
    public abstract static class FullState {
        final Semaphore backendPermits = new Semaphore(0);
        MicroBatcher<Integer, Integer> batcher;
        CompletableFuture<Integer> occupied;

        abstract AdmissionPolicy policy();

        abstract Duration timeout();

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            var config = BenchmarkSupport.config(
                    1, Duration.ZERO, 1, 1, policy(), timeout());
            batcher = new MicroBatcher<>(config, ignored -> {
                backendPermits.acquire();
                return RESULT;
            });
            occupied = batcher.submit(BenchmarkSupport.VALUE);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            backendPermits.release();
            occupied.join();
            batcher.close();
        }
    }

    @State(Scope.Thread)
    public static class RejectState extends FullState {
        @Override
        AdmissionPolicy policy() {
            return AdmissionPolicy.REJECT;
        }

        @Override
        Duration timeout() {
            return Duration.ZERO;
        }
    }

    @State(Scope.Thread)
    public static class TimedWaitState extends FullState {
        @Override
        AdmissionPolicy policy() {
            return AdmissionPolicy.WAIT_WITH_TIMEOUT;
        }

        @Override
        Duration timeout() {
            return Duration.ofNanos(100_000);
        }
    }

    @State(Scope.Thread)
    public static class WaitState {
        private final Semaphore backendPermits = new Semaphore(0);
        private final Semaphore releaseRequests = new Semaphore(0);
        private volatile boolean running;
        private volatile Thread waitingThread;
        private MicroBatcher<Integer, Integer> batcher;
        private CompletableFuture<Integer> occupied;
        private Thread releaser;

        @Setup(Level.Trial)
        public void setUpTrial() {
            var config = BenchmarkSupport.config(
                    1, Duration.ZERO, 1, 1, AdmissionPolicy.WAIT, Duration.ZERO);
            batcher = new MicroBatcher<>(config, ignored -> {
                backendPermits.acquire();
                return RESULT;
            });
            running = true;
            releaser = Thread.ofPlatform().daemon(true).start(this::releaseWhenWaiting);
        }

        @Setup(Level.Invocation)
        public void setUpInvocation() throws Exception {
            occupied = batcher.submit(BenchmarkSupport.VALUE);
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() throws InterruptedException {
            running = false;
            releaseRequests.release();
            releaser.join();
            batcher.close();
        }

        private void releaseWhenWaiting() {
            while (running) {
                releaseRequests.acquireUninterruptibly();
                if (!running) {
                    return;
                }
                while (running) {
                    Thread thread = waitingThread;
                    if (thread != null && thread.getState() == Thread.State.WAITING) {
                        backendPermits.release(2);
                        break;
                    }
                    Thread.onSpinWait();
                }
            }
        }
    }

    @State(Scope.Thread)
    public static class OccupancyState {
        @Param({"0", "32", "63"})
        public int occupancy;

        private final Semaphore backendPermits = new Semaphore(0);
        private MicroBatcher<Integer, Integer> batcher;
        private CompletableFuture<Integer>[] occupied;
        private CompletableFuture<Integer> admitted;

        @SuppressWarnings("unchecked")
        @Setup(Level.Trial)
        public void setUpTrial() {
            var config = BenchmarkSupport.config(
                    1, Duration.ZERO, 1, 64, AdmissionPolicy.WAIT, Duration.ZERO);
            batcher = new MicroBatcher<>(config, ignored -> {
                backendPermits.acquire();
                return RESULT;
            });
            occupied = (CompletableFuture<Integer>[]) new CompletableFuture<?>[occupancy];
        }

        @Setup(Level.Invocation)
        public void setUpInvocation() throws Exception {
            for (int index = 0; index < occupancy; index++) {
                occupied[index] = batcher.submit(index);
            }
        }

        @TearDown(Level.Invocation)
        public void tearDownInvocation() {
            backendPermits.release(occupancy + 1);
            if (admitted.join() != BenchmarkSupport.VALUE) {
                throw new IllegalStateException("admitted request completed with the wrong value");
            }
            for (var future : occupied) {
                future.join();
            }
            admitted = null;
        }

        @TearDown(Level.Trial)
        public void tearDownTrial() {
            batcher.close();
        }
    }

    @Benchmark
    public Class<?> rejectAtCapacity(RejectState state) throws Exception {
        try {
            state.batcher.submit(BenchmarkSupport.VALUE);
            throw new AssertionError("capacity rejection was not enforced");
        } catch (RejectedExecutionException expected) {
            return expected.getClass();
        }
    }

    @Benchmark
    public Class<?> timedWaitAtCapacity(TimedWaitState state) throws Exception {
        try {
            state.batcher.submit(BenchmarkSupport.VALUE);
            throw new AssertionError("timed admission did not expire");
        } catch (TimeoutException expected) {
            return expected.getClass();
        }
    }

    @Benchmark
    public int waitForCapacity(WaitState state) throws Exception {
        state.waitingThread = Thread.currentThread();
        state.releaseRequests.release();
        try {
            var admitted = state.batcher.submit(BenchmarkSupport.VALUE);
            return state.occupied.join() + admitted.join();
        } finally {
            state.waitingThread = null;
        }
    }

    @Benchmark
    public CompletableFuture<Integer> admitAtOccupancy(OccupancyState state) throws Exception {
        state.admitted = state.batcher.submit(BenchmarkSupport.VALUE);
        return state.admitted;
    }
}
