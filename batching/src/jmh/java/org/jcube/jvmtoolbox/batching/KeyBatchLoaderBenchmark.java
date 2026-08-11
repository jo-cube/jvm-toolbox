package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
public class KeyBatchLoaderBenchmark {
    private static final int REQUESTS = 32;

    @Param({"32", "16", "4"})
    public int uniqueKeys;

    @Param({"false", "true"})
    public boolean statisticsEnabled;

    private Integer[] keys;
    private CompletableFuture<Optional<Integer>>[] futures;
    private KeyBatchLoader<Integer, Integer> loader;
    private BatchStatistics statistics;

    @SuppressWarnings("unchecked")
    @Setup(Level.Trial)
    public void setUp() {
        keys = new Integer[REQUESTS];
        var mutableResults = new LinkedHashMap<Integer, BatchOutcome<Integer>>();
        for (int index = 0; index < REQUESTS; index++) {
            int key = index % uniqueKeys;
            keys[index] = key;
            mutableResults.put(key, BatchOutcome.success(key));
        }
        Map<Integer, BatchOutcome<Integer>> results = Map.copyOf(mutableResults);
        futures = (CompletableFuture<Optional<Integer>>[]) new CompletableFuture<?>[REQUESTS];
        var config = BenchmarkSupport.config(
                REQUESTS,
                Duration.ofSeconds(5),
                1,
                REQUESTS * 2,
                AdmissionPolicy.WAIT,
                Duration.ZERO);
        KeyBatchProcessor<Integer, Integer> processor = requested -> validateAndReturn(requested, results);
        if (statisticsEnabled) {
            statistics = new BatchStatistics();
            loader = new KeyBatchLoader<>(config, processor, statistics);
        } else {
            loader = new KeyBatchLoader<>(config, processor);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        loader.close();
        if (statistics != null) {
            var snapshot = statistics.snapshot();
            long expectedUniqueKeys = snapshot.keyedRequests() / REQUESTS * uniqueKeys;
            if (snapshot.incompleteRequests() != 0
                    || snapshot.keyedRequests() == 0
                    || snapshot.uniqueKeys() != expectedUniqueKeys) {
                throw new IllegalStateException("observed keyed benchmark counters do not match its work");
            }
        }
    }

    @Benchmark
    public int loadBatch() throws Exception {
        for (int index = 0; index < REQUESTS; index++) {
            futures[index] = loader.load(keys[index]);
        }
        int sum = 0;
        for (var future : futures) {
            sum += future.join().orElseThrow();
        }
        return sum;
    }

    private static Map<Integer, BatchOutcome<Integer>> validateAndReturn(
            Set<Integer> requested, Map<Integer, BatchOutcome<Integer>> results) {
        if (!requested.equals(results.keySet())) {
            throw new IllegalStateException("formed keyed batch did not contain the expected unique keys");
        }
        return results;
    }
}
