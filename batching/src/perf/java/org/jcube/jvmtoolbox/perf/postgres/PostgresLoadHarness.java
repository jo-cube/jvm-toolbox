package org.jcube.jvmtoolbox.perf.postgres;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.jcube.jvmtoolbox.batching.AdmissionPolicy;
import org.jcube.jvmtoolbox.batching.BatchOutcome;
import org.jcube.jvmtoolbox.batching.BatchingConfig;
import org.jcube.jvmtoolbox.batching.KeyBatchLoader;
import org.jcube.jvmtoolbox.batching.MicroBatcher;
import org.jcube.jvmtoolbox.perf.backend.BackendProfiler;
import org.jcube.jvmtoolbox.perf.backend.BackendProfilerReport;
import org.jcube.jvmtoolbox.perf.backend.ParetoFrontier;

public final class PostgresLoadHarness {
    private static final Path REPORT_DIRECTORY = Path.of("build", "reports", "perf");
    private static final int CAPACITY = 32_768;
    private static final Duration MAX_WAIT = Duration.ofNanos(500_000);
    private static final double PARETO_THROUGHPUT_TOLERANCE = 0.05;
    private static final double PARETO_LATENCY_TOLERANCE = 0.05;

    private PostgresLoadHarness() {}

    public static void main(String[] args) throws Exception {
        String profile = args.length == 0 ? "quick" : args[0];
        var settings = PostgresSettings.environment();
        DatasetInfo dataset = JdbcLookupBackend.datasetInfo(settings);
        if (dataset.rows() == 0) {
            throw new IllegalStateException("lookup_value is empty; run just postgres-seed first");
        }
        Files.createDirectories(REPORT_DIRECTORY);
        System.out.printf(
                Locale.ROOT,
                "PostgreSQL dataset: %,d rows, %.1f MiB%n",
                dataset.rows(),
                dataset.relationBytes() / 1_048_576d);

        if (profile.equals("plan")) {
            capturePlan(settings, dataset.rows(), environmentInt("JVM_TOOLBOX_POSTGRES_BATCH", 128));
            return;
        }
        if (profile.equals("profile")) {
            int batchSize = environmentInt("JVM_TOOLBOX_POSTGRES_BATCH", 128);
            int concurrency = environmentInt("JVM_TOOLBOX_POSTGRES_CONCURRENCY", 8);
            Scenario scenario = Scenario.loader(
                    "profile-loader-hit-unique",
                    Pattern.HIT_UNIQUE,
                    batchSize,
                    concurrency,
                    10_000,
                    Duration.ofSeconds(5));
            DbRunResult result = run(settings, dataset, scenario);
            result.validate();
            print(result);
            write(profile, List.of(result));
            capturePlan(settings, dataset.rows(), batchSize);
            return;
        }
        if (profile.equals("attribution") || profile.equals("attribution-profile")) {
            runAttribution(settings, dataset, profile);
            return;
        }

        boolean full = profile.equals("full");
        if (!full && !profile.equals("quick")) {
            throw new IllegalArgumentException("unknown PostgreSQL profile: " + profile);
        }
        Duration sweepDuration = Duration.ofSeconds(full ? 2 : 1);
        Duration comparisonDuration = Duration.ofSeconds(full ? 4 : 2);
        int callers = full ? 20_000 : 5_000;
        List<DbRunResult> results = new ArrayList<>();

        BackendSweep sweep = profileBackend(settings, dataset, sweepDuration, full);
        results.addAll(sweep.runs());
        for (DbRunResult result : sweep.runs()) {
            result.validate();
        }
        for (BackendProfiler.AggregatedResult result : sweep.aggregates()) {
            print(result, sweep.frontier().contains(result));
        }
        writeBackendSweep(profile, sweep.repetitions(), sweep.aggregates(), sweep.frontier());

        BackendProfiler.AggregatedResult operatingPoint = sweep.aggregates().stream()
                .max((left, right) -> Double.compare(
                        left.logicalOperationsPerSecond(), right.logicalOperationsPerSecond()))
                .orElseThrow();
        int batchSize = operatingPoint.batchSize();
        int concurrency = operatingPoint.backendConcurrency();
        System.out.printf(
                Locale.ROOT,
                "operating point: batch=%d databaseConcurrency=%d%n",
                batchSize,
                concurrency);

        DbRunResult hitLoader = null;
        for (Pattern pattern : Pattern.values()) {
            DbRunResult direct = run(
                    settings,
                    dataset,
                    Scenario.direct(
                            "direct-" + pattern.label,
                            pattern,
                            batchSize,
                            concurrency,
                            comparisonDuration));
            direct.validate();
            results.add(direct);
            print(direct);

            DbRunResult loader = run(
                    settings,
                    dataset,
                    Scenario.loader(
                            "loader-" + pattern.label,
                            pattern,
                            batchSize,
                            concurrency,
                            callers,
                            comparisonDuration));
            loader.validate();
            loader = loader.withThroughputRatio(loader.logicalRequestsPerSecond() / direct.logicalRequestsPerSecond());
            results.add(loader);
            print(loader);
            if (pattern == Pattern.HIT_UNIQUE) {
                hitLoader = loader;
            }
        }

        long recoveryRate = Math.max(1_000, Math.round(hitLoader.logicalRequestsPerSecond() * 0.8));
        Scenario recovery = Scenario.recovery(
                "loader-slowdown-recovery",
                batchSize,
                concurrency,
                recoveryRate,
                full ? Duration.ofSeconds(9) : Duration.ofSeconds(6),
                full
                        ? new Slowdown(Duration.ofSeconds(3), Duration.ofSeconds(3), Duration.ofMillis(20))
                        : new Slowdown(Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofMillis(20)));
        DbRunResult recoveryResult = run(settings, dataset, recovery);
        recoveryResult.validate();
        results.add(recoveryResult);
        print(recoveryResult);

        write(profile, results);
        capturePlan(settings, dataset.rows(), batchSize);
    }

    private static void runAttribution(
            PostgresSettings settings, DatasetInfo dataset, String profile) throws Exception {
        boolean profiling = profile.equals("attribution-profile");
        Duration duration = Duration.ofSeconds(profiling ? 4 : 3);
        List<Integer> batchSizes = profiling ? List.of(128) : List.of(128, 512);
        var results = new ArrayList<DbRunResult>();
        warmup(settings, dataset);
        for (int batchSize : batchSizes) {
            int outstanding = batchSize * 8;
            DbRunResult direct = run(
                    settings,
                    dataset,
                    Scenario.direct(
                            "attribution-b" + batchSize + "-direct",
                            Pattern.HIT_UNIQUE,
                            batchSize,
                            8,
                            duration));
            direct.validate();
            results.add(direct);
            print(direct);

            for (Scenario scenario : List.of(
                    Scenario.microAsync(
                            "attribution-b" + batchSize + "-micro-async",
                            batchSize,
                            8,
                            outstanding,
                            duration),
                    Scenario.loaderAsync(
                            "attribution-b" + batchSize + "-keyed-async",
                            batchSize,
                            8,
                            outstanding,
                            duration),
                    Scenario.loader(
                            "attribution-b" + batchSize + "-keyed-blocking",
                            Pattern.HIT_UNIQUE,
                            batchSize,
                            8,
                            outstanding,
                            duration))) {
                DbRunResult result = run(settings, dataset, scenario);
                result.validate();
                result = result.withThroughputRatio(
                        result.logicalRequestsPerSecond() / direct.logicalRequestsPerSecond());
                results.add(result);
                print(result);
            }
        }
        write(profile, results);
    }

    private static void warmup(PostgresSettings settings, DatasetInfo dataset) throws Exception {
        run(
                settings,
                dataset,
                Scenario.direct(
                        "warmup", Pattern.HIT_UNIQUE, 128, 4, Duration.ofSeconds(1)));
    }

    private static BackendSweep profileBackend(
            PostgresSettings settings, DatasetInfo dataset, Duration duration, boolean full)
            throws Exception {
        var config = new BackendProfiler.Config(
                List.of(32, 128, 512),
                List.of(1, 4, 8),
                Duration.ofMillis(full ? 500 : 250),
                duration,
                3);
        var profiler = new BackendProfiler<LookupKey>(
                config,
                (batchSize, batchSequence) ->
                        profileInputs(batchSize, batchSequence, dataset.rows()),
                concurrency -> {
                    var jdbc = new JdbcLookupBackend(settings, concurrency);
                    return new BackendProfiler.BatchBackend<>() {
                        @Override
                        public BackendProfiler.BatchResult process(List<LookupKey> batch)
                                throws Exception {
                            Map<LookupKey, BatchOutcome<BigDecimal>> values = jdbc.load(batch);
                            int successful = 0;
                            int missing = 0;
                            for (LookupKey key : batch) {
                                if (validate(key, values.get(key), Pattern.HIT_UNIQUE)) {
                                    successful++;
                                } else {
                                    missing++;
                                }
                            }
                            return new BackendProfiler.BatchResult(successful, missing, 0);
                        }

                        @Override
                        public void close() {
                            jdbc.close();
                        }
                    };
                });
        List<BackendProfiler.Result> repetitions = profiler.profile();
        List<BackendProfiler.AggregatedResult> aggregates =
                BackendProfiler.aggregate(repetitions);
        List<BackendProfiler.AggregatedResult> frontier = ParetoFrontier.efficient(
                aggregates,
                List.of(
                        ParetoFrontier.Objective.maximize(
                                BackendProfiler.AggregatedResult::logicalOperationsPerSecond,
                                ParetoFrontier.Tolerance.relative(
                                        PARETO_THROUGHPUT_TOLERANCE)),
                        ParetoFrontier.Objective.minimize(
                                result -> result.backendLatency().p99(),
                                ParetoFrontier.Tolerance.relative(
                                        PARETO_LATENCY_TOLERANCE)),
                        ParetoFrontier.Objective.minimize(
                                BackendProfiler.AggregatedResult::backendConcurrency)));

        var runs = new ArrayList<DbRunResult>(repetitions.size());
        for (BackendProfiler.Result result : repetitions) {
            runs.add(DbRunResult.fromProfile(dataset, duration, result));
        }
        System.out.printf(
                Locale.ROOT,
                "PostgreSQL Pareto tolerances: throughput=%.1f%% latency=%.1f%%%n",
                PARETO_THROUGHPUT_TOLERANCE * 100,
                PARETO_LATENCY_TOLERANCE * 100);
        System.out.println("PostgreSQL tolerance-aware Pareto configurations: " + frontier.stream()
                .map(result -> "b" + result.batchSize() + "/c" + result.backendConcurrency())
                .toList());
        return new BackendSweep(List.copyOf(runs), repetitions, aggregates, frontier);
    }

    private static List<LookupKey> profileInputs(
            int batchSize, long batchSequence, long datasetRows) {
        long base = batchSequence * batchSize;
        var keys = new ArrayList<LookupKey>(batchSize);
        for (int index = 0; index < batchSize; index++) {
            keys.add(Pattern.HIT_UNIQUE.key(base + index, datasetRows));
        }
        return keys;
    }

    private static DbRunResult run(PostgresSettings settings, DatasetInfo dataset, Scenario scenario)
            throws Exception {
        System.gc();
        var metrics = new PostgresMetrics();
        var probe = new JvmProbe(metrics);
        try (var backend = new JdbcLookupBackend(
                settings,
                scenario.databaseConcurrency(),
                metrics,
                scenario.slowdown(),
                metrics::elapsedNanos)) {
            long started;
            started = switch (scenario.mode()) {
                case DIRECT -> runDirect(scenario, dataset.rows(), backend, metrics, probe);
                case MICRO_ASYNC -> runMicroAsync(scenario, dataset.rows(), backend, metrics, probe);
                case LOADER_ASYNC -> runLoaderAsync(scenario, dataset.rows(), backend, metrics, probe);
                case LOADER -> runLoader(scenario, dataset.rows(), backend, metrics, probe);
                case RECOVERY -> runRecovery(scenario, dataset.rows(), backend, metrics, probe);
            };
            long ended = System.nanoTime();
            return new DbRunResult(
                    scenario,
                    dataset,
                    metrics.snapshot(),
                    probe.finish(ended),
                    started,
                    0);
        }
    }

    private static long runDirect(
            Scenario scenario,
            long datasetRows,
            JdbcLookupBackend backend,
            PostgresMetrics metrics,
            JvmProbe probe)
            throws Exception {
        var ready = new CountDownLatch(scenario.databaseConcurrency());
        var startGate = new CountDownLatch(1);
        var done = new CountDownLatch(scenario.databaseConcurrency());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int worker = 0; worker < scenario.databaseConcurrency(); worker++) {
                List<PreparedBatch> batches = prepareBatches(scenario, datasetRows, worker);
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        startGate.await();
                        int index = 0;
                        while (metrics.elapsedNanos() < scenario.duration().toNanos()) {
                            executeDirect(batches.get(index++ % batches.size()), backend, metrics, scenario.pattern());
                        }
                    } catch (Throwable failure) {
                        metrics.unexpected(failure);
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            long started = System.nanoTime();
            metrics.start(started);
            probe.start(started);
            startGate.countDown();
            done.await();
            return started;
        }
    }

    private static void executeDirect(
            PreparedBatch batch,
            JdbcLookupBackend backend,
            PostgresMetrics metrics,
            Pattern pattern) {
        long started = System.nanoTime();
        metrics.directBatchStarted(batch.logical().size(), batch.unique().size());
        try {
            Map<LookupKey, BatchOutcome<BigDecimal>> values = backend.load(batch.unique());
            int found = 0;
            int missing = 0;
            for (LookupKey key : batch.logical()) {
                if (validate(key, values.get(key), pattern)) {
                    found++;
                } else {
                    missing++;
                }
            }
            metrics.directBatchCompleted(started, found, missing);
        } catch (Throwable failure) {
            metrics.directBatchFailed(started, batch.logical().size());
            metrics.unexpected(failure);
        }
    }

    private static long runMicroAsync(
            Scenario scenario,
            long datasetRows,
            JdbcLookupBackend backend,
            PostgresMetrics metrics,
            JvmProbe probe)
            throws Exception {
        var config = attributionConfig(scenario);
        var done = new CountDownLatch(scenario.callers());
        var ids = new AtomicLong();
        int completionThreads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        try (var batcher = new MicroBatcher<LookupKey, BigDecimal>(
                        config, keys -> loadPositionally(keys, backend, metrics), metrics);
                var completions = Executors.newFixedThreadPool(completionThreads)) {
            long started = System.nanoTime();
            long deadline = started + scenario.duration().toNanos();
            metrics.start(started);
            probe.start(started);
            for (int lane = 0; lane < scenario.callers(); lane++) {
                metrics.callerStarted();
                submitMicroAsync(
                        batcher,
                        completions,
                        ids,
                        datasetRows,
                        deadline,
                        metrics,
                        done);
            }
            done.await();
            return started;
        }
    }

    private static List<BatchOutcome<BigDecimal>> loadPositionally(
            List<LookupKey> keys, JdbcLookupBackend backend, PostgresMetrics metrics) throws Exception {
        Set<LookupKey> unique = Set.copyOf(keys);
        metrics.genericKeysDispatched(unique.size());
        Map<LookupKey, BatchOutcome<BigDecimal>> loaded = backend.load(unique);
        var outcomes = new ArrayList<BatchOutcome<BigDecimal>>(keys.size());
        for (LookupKey key : keys) {
            BatchOutcome<BigDecimal> outcome = loaded.get(key);
            if (outcome == null) {
                throw new IllegalStateException("database returned no value for " + key);
            }
            outcomes.add(outcome);
        }
        return outcomes;
    }

    private static void submitMicroAsync(
            MicroBatcher<LookupKey, BigDecimal> batcher,
            java.util.concurrent.Executor completionExecutor,
            AtomicLong ids,
            long datasetRows,
            long deadline,
            PostgresMetrics metrics,
            CountDownLatch done) {
        if (System.nanoTime() >= deadline || metrics.hasUnexpected()) {
            metrics.callerFinished();
            done.countDown();
            return;
        }
        LookupKey key = Pattern.HIT_UNIQUE.key(ids.getAndIncrement(), datasetRows);
        long submitted = System.nanoTime();
        metrics.attempt();
        try {
            batcher.submit(key).whenCompleteAsync((value, failure) -> {
                if (failure == null) {
                    validate(key, value);
                    metrics.logicalCompleted(submitted, true);
                    submitMicroAsync(
                            batcher,
                            completionExecutor,
                            ids,
                            datasetRows,
                            deadline,
                            metrics,
                            done);
                } else {
                    metrics.logicalFailed(submitted);
                    metrics.unexpected(failure);
                    metrics.callerFinished();
                    done.countDown();
                }
            }, completionExecutor);
        } catch (Throwable failure) {
            metrics.unexpected(failure);
            metrics.callerFinished();
            done.countDown();
        }
    }

    private static long runLoaderAsync(
            Scenario scenario,
            long datasetRows,
            JdbcLookupBackend backend,
            PostgresMetrics metrics,
            JvmProbe probe)
            throws Exception {
        var done = new CountDownLatch(scenario.callers());
        var ids = new AtomicLong();
        int completionThreads = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
        try (var loader = new KeyBatchLoader<LookupKey, BigDecimal>(
                        attributionConfig(scenario), backend::load, metrics);
                var completions = Executors.newFixedThreadPool(completionThreads)) {
            long started = System.nanoTime();
            long deadline = started + scenario.duration().toNanos();
            metrics.start(started);
            probe.start(started);
            for (int lane = 0; lane < scenario.callers(); lane++) {
                metrics.callerStarted();
                submitLoaderAsync(
                        loader,
                        completions,
                        ids,
                        datasetRows,
                        deadline,
                        metrics,
                        done);
            }
            done.await();
            return started;
        }
    }

    private static void submitLoaderAsync(
            KeyBatchLoader<LookupKey, BigDecimal> loader,
            java.util.concurrent.Executor completionExecutor,
            AtomicLong ids,
            long datasetRows,
            long deadline,
            PostgresMetrics metrics,
            CountDownLatch done) {
        if (System.nanoTime() >= deadline || metrics.hasUnexpected()) {
            metrics.callerFinished();
            done.countDown();
            return;
        }
        LookupKey key = Pattern.HIT_UNIQUE.key(ids.getAndIncrement(), datasetRows);
        long submitted = System.nanoTime();
        metrics.attempt();
        try {
            loader.load(key).whenCompleteAsync((value, failure) -> {
                if (failure == null) {
                    validate(key, value, Pattern.HIT_UNIQUE);
                    metrics.logicalCompleted(submitted, true);
                    submitLoaderAsync(
                            loader,
                            completionExecutor,
                            ids,
                            datasetRows,
                            deadline,
                            metrics,
                            done);
                } else {
                    metrics.logicalFailed(submitted);
                    metrics.unexpected(failure);
                    metrics.callerFinished();
                    done.countDown();
                }
            }, completionExecutor);
        } catch (Throwable failure) {
            metrics.unexpected(failure);
            metrics.callerFinished();
            done.countDown();
        }
    }

    private static BatchingConfig attributionConfig(Scenario scenario) {
        return new BatchingConfig(
                scenario.batchSize(),
                MAX_WAIT,
                scenario.databaseConcurrency(),
                scenario.capacity(),
                AdmissionPolicy.REJECT,
                Duration.ZERO);
    }

    private static long runLoader(
            Scenario scenario,
            long datasetRows,
            JdbcLookupBackend backend,
            PostgresMetrics metrics,
            JvmProbe probe)
            throws Exception {
        var config = new BatchingConfig(
                scenario.batchSize(),
                MAX_WAIT,
                scenario.databaseConcurrency(),
                scenario.capacity(),
                scenario.policy(),
                Duration.ofMillis(5));
        var ready = new CountDownLatch(scenario.callers());
        var startGate = new CountDownLatch(1);
        var done = new CountDownLatch(scenario.callers());
        var ids = new AtomicLong();
        try (var loader = new KeyBatchLoader<LookupKey, BigDecimal>(config, backend::load, metrics);
                var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int caller = 0; caller < scenario.callers(); caller++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        startGate.await();
                        metrics.callerStarted();
                        while (metrics.elapsedNanos() < scenario.duration().toNanos()) {
                            executeLoad(
                                    loader,
                                    scenario.pattern().key(ids.getAndIncrement(), datasetRows),
                                    scenario.pattern(),
                                    metrics,
                                    System.nanoTime());
                        }
                    } catch (Throwable failure) {
                        metrics.unexpected(failure);
                    } finally {
                        metrics.callerFinished();
                        done.countDown();
                    }
                });
            }
            if (!ready.await(2, TimeUnit.MINUTES)) {
                throw new IllegalStateException("foreground callers did not start");
            }
            long started = System.nanoTime();
            metrics.start(started);
            probe.start(started);
            startGate.countDown();
            done.await();
            return started;
        }
    }

    private static long runRecovery(
            Scenario scenario,
            long datasetRows,
            JdbcLookupBackend backend,
            PostgresMetrics metrics,
            JvmProbe probe)
            throws Exception {
        var config = new BatchingConfig(
                scenario.batchSize(),
                MAX_WAIT,
                scenario.databaseConcurrency(),
                scenario.capacity(),
                AdmissionPolicy.REJECT,
                Duration.ZERO);
        long requestCount = scenario.arrivalRate() * scenario.duration().toNanos() / 1_000_000_000L;
        if (requestCount > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("recovery workload is too large");
        }
        var completed = new CountDownLatch((int) requestCount);
        try (var loader = new KeyBatchLoader<LookupKey, BigDecimal>(config, backend::load, metrics)) {
            long started = System.nanoTime();
            metrics.start(started);
            probe.start(started);
            for (long sequence = 0; sequence < requestCount; sequence++) {
                long scheduled = started + sequence * 1_000_000_000L / scenario.arrivalRate();
                waitUntil(scheduled);
                LookupKey key = scenario.pattern().key(sequence, datasetRows);
                metrics.attempt();
                try {
                    CompletableFuture<Optional<BigDecimal>> future = loader.load(key);
                    metrics.callerStarted();
                    future.whenComplete((value, failure) -> {
                        try {
                            if (failure == null) {
                                validate(key, value, scenario.pattern());
                                metrics.logicalCompleted(scheduled, value.isPresent());
                            } else {
                                metrics.logicalFailed(scheduled);
                                metrics.unexpected(failure);
                            }
                        } finally {
                            metrics.callerFinished();
                            completed.countDown();
                        }
                    });
                } catch (RejectedExecutionException | TimeoutException expected) {
                    completed.countDown();
                }
            }
            completed.await();
            return started;
        }
    }

    private static void executeLoad(
            KeyBatchLoader<LookupKey, BigDecimal> loader,
            LookupKey key,
            Pattern pattern,
            PostgresMetrics metrics,
            long started)
            throws InterruptedException {
        metrics.attempt();
        try {
            Optional<BigDecimal> value = loader.load(key).join();
            validate(key, value, pattern);
            metrics.logicalCompleted(started, value.isPresent());
        } catch (RejectedExecutionException | TimeoutException expected) {
        } catch (CompletionException failure) {
            metrics.logicalFailed(started);
            throw failure;
        }
    }

    private static boolean validate(
            LookupKey key, BatchOutcome<BigDecimal> outcome, Pattern pattern) {
        Optional<BigDecimal> expected = pattern.expected(key);
        if (expected.isEmpty()) {
            if (outcome != null) {
                throw new IllegalStateException("database returned a value for missing key " + key);
            }
            return false;
        }
        if (!(outcome instanceof BatchOutcome.Success<?> success)
                || !(success.value() instanceof BigDecimal value)
                || value.compareTo(expected.orElseThrow()) != 0) {
            throw new IllegalStateException("incorrect database result for " + key);
        }
        return true;
    }

    private static void validate(LookupKey key, Optional<BigDecimal> value, Pattern pattern) {
        Optional<BigDecimal> expected = pattern.expected(key);
        if (value.isPresent() != expected.isPresent()
                || value.isPresent() && value.orElseThrow().compareTo(expected.orElseThrow()) != 0) {
            throw new IllegalStateException("incorrect loader result for " + key);
        }
    }

    private static void validate(LookupKey key, BigDecimal value) {
        if (value.compareTo(Pattern.HIT_UNIQUE.expected(key).orElseThrow()) != 0) {
            throw new IllegalStateException("incorrect micro-batcher result for " + key);
        }
    }

    private static List<PreparedBatch> prepareBatches(Scenario scenario, long datasetRows, int worker) {
        var batches = new ArrayList<PreparedBatch>(32);
        long base = (long) worker * scenario.batchSize() * 32;
        for (int batch = 0; batch < 32; batch++) {
            var logical = new ArrayList<LookupKey>(scenario.batchSize());
            for (int index = 0; index < scenario.batchSize(); index++) {
                logical.add(scenario.pattern().key(base + (long) batch * scenario.batchSize() + index, datasetRows));
            }
            batches.add(new PreparedBatch(List.copyOf(logical), Set.copyOf(new LinkedHashSet<>(logical))));
        }
        return List.copyOf(batches);
    }

    private static void waitUntil(long deadline) {
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            if (remaining > 50_000) {
                LockSupport.parkNanos(remaining - 20_000);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private static void capturePlan(PostgresSettings settings, long datasetRows, int batchSize)
            throws IOException, java.sql.SQLException {
        var keys = new LinkedHashSet<LookupKey>();
        for (int index = 0; index < batchSize; index++) {
            keys.add(Pattern.HIT_UNIQUE.key(index, datasetRows));
        }
        List<String> plan = JdbcLookupBackend.explain(settings, keys);
        Path path = REPORT_DIRECTORY.resolve("postgres-plan.txt");
        Files.write(path, plan);
        System.out.println("execution plan: " + path);
    }

    private static void print(DbRunResult result) {
        MetricsSnapshot metrics = result.metrics();
        System.out.printf(
                Locale.ROOT,
                "%-30s logical=%9.0f/s unique=%9.0f/s db=%7.0f batches/s batch=%6.1f/%6.1f dbP99=%6.2fms logicalP99=%7.2fms ratio=%5.1f%% reject=%5.1f%%%n",
                result.scenario().name(),
                result.logicalRequestsPerSecond(),
                result.uniqueKeysPerSecond(),
                result.databaseBatchesPerSecond(),
                result.averageLogicalBatch(),
                result.averageUniqueBatch(),
                metrics.databaseLatency().p99() / 1_000_000d,
                metrics.logicalLatency().p99() / 1_000_000d,
                result.throughputRatio() * 100,
                result.rejectionPercent());
    }

    private static void print(BackendProfiler.AggregatedResult result, boolean efficient) {
        System.out.printf(
                Locale.ROOT,
                "direct-sweep-b%-3d-c%-2d          logical=%9.0f/s spread=%4.1f%% dbP99=%6.2fms repetitions=%d pareto=%s%n",
                result.batchSize(),
                result.backendConcurrency(),
                result.logicalOperationsPerSecond(),
                result.stability().relativeSpread() * 100,
                result.backendLatency().p99() / 1_000_000d,
                result.repetitionCount(),
                efficient);
    }

    private static void write(String profile, List<DbRunResult> results) throws IOException {
        Path resultPath = REPORT_DIRECTORY.resolve("postgres-" + profile + "-results.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(resultPath)) {
            writer.write(DbRunResult.header());
            writer.newLine();
            for (DbRunResult result : results) {
                writer.write(result.csv());
                writer.newLine();
            }
        }
        Path samplePath = REPORT_DIRECTORY.resolve("postgres-" + profile + "-samples.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(samplePath)) {
            writer.write("scenario,seconds,attempts,completed,rejected,timeouts,pending,in_flight,database_active,active_callers,heap_bytes,platform_threads");
            writer.newLine();
            for (DbRunResult result : results) {
                for (MetricSample sample : result.resources().samples()) {
                    writer.write(String.join(",", List.of(
                            result.scenario().name(),
                            Double.toString(sample.seconds()),
                            Long.toString(sample.attempts()),
                            Long.toString(sample.completed()),
                            Long.toString(sample.rejected()),
                            Long.toString(sample.timedOut()),
                            Integer.toString(sample.pending()),
                            Integer.toString(sample.inFlight()),
                            Integer.toString(sample.databaseActive()),
                            Integer.toString(sample.activeCallers()),
                            Long.toString(sample.heapBytes()),
                            Integer.toString(sample.platformThreads()))));
                    writer.newLine();
                }
            }
        }
        System.out.println("reports: " + resultPath + " and " + samplePath);
    }

    private static void writeBackendSweep(
            String profile,
            List<BackendProfiler.Result> repetitions,
            List<BackendProfiler.AggregatedResult> aggregates,
            List<BackendProfiler.AggregatedResult> frontier)
            throws IOException {
        Path path = REPORT_DIRECTORY.resolve("postgres-" + profile + "-backend-profiler.csv");
        BackendProfilerReport.writeCsv(path, repetitions, aggregates, frontier);
        System.out.println("backend profiler report: " + path);
    }

    private static int environmentInt(String name, int fallback) {
        return Integer.parseInt(System.getenv().getOrDefault(name, Integer.toString(fallback)));
    }

    enum Mode {
        DIRECT,
        MICRO_ASYNC,
        LOADER_ASYNC,
        LOADER,
        RECOVERY
    }

    enum Pattern {
        HIT_UNIQUE("hit-unique") {
            @Override
            LookupKey key(long sequence, long rows) {
                return presentKey(Math.floorMod(sequence, rows));
            }
        },
        MISSING_MIX("missing-mix") {
            @Override
            LookupKey key(long sequence, long rows) {
                return sequence % 10 == 0
                        ? new LookupKey("missing-" + sequence, -1)
                        : presentKey(Math.floorMod(sequence, rows));
            }
        },
        DUPLICATE_HEAVY("duplicate-heavy") {
            @Override
            LookupKey key(long sequence, long rows) {
                return presentKey(Math.floorMod(sequence, Math.min(32, rows)));
            }
        };

        private final String label;

        Pattern(String label) {
            this.label = label;
        }

        abstract LookupKey key(long sequence, long rows);

        Optional<BigDecimal> expected(LookupKey key) {
            if (key.number() < 0) {
                return Optional.empty();
            }
            long tenant = Long.parseLong(key.text().substring("tenant-".length()));
            return Optional.of(BigDecimal.valueOf(tenant * 1000 + key.number(), 2));
        }

        static LookupKey presentKey(long ordinal) {
            return new LookupKey("tenant-" + ordinal / 1000, (int) (ordinal % 1000));
        }
    }

    record Scenario(
            String name,
            Mode mode,
            Pattern pattern,
            int batchSize,
            int databaseConcurrency,
            int callers,
            long arrivalRate,
            Duration duration,
            int capacity,
            AdmissionPolicy policy,
            Slowdown slowdown) {

        static Scenario direct(
                String name,
                Pattern pattern,
                int batchSize,
                int concurrency,
                Duration duration) {
            return new Scenario(
                    name,
                    Mode.DIRECT,
                    pattern,
                    batchSize,
                    concurrency,
                    concurrency,
                    0,
                    duration,
                    0,
                    AdmissionPolicy.WAIT,
                    Slowdown.NONE);
        }

        static Scenario loader(
                String name,
                Pattern pattern,
                int batchSize,
                int concurrency,
                int callers,
                Duration duration) {
            return new Scenario(
                    name,
                    Mode.LOADER,
                    pattern,
                    batchSize,
                    concurrency,
                    callers,
                    0,
                    duration,
                    CAPACITY,
                    AdmissionPolicy.WAIT,
                    Slowdown.NONE);
        }

        static Scenario microAsync(
                String name,
                int batchSize,
                int concurrency,
                int outstanding,
                Duration duration) {
            return asynchronous(name, Mode.MICRO_ASYNC, batchSize, concurrency, outstanding, duration);
        }

        static Scenario loaderAsync(
                String name,
                int batchSize,
                int concurrency,
                int outstanding,
                Duration duration) {
            return asynchronous(name, Mode.LOADER_ASYNC, batchSize, concurrency, outstanding, duration);
        }

        private static Scenario asynchronous(
                String name,
                Mode mode,
                int batchSize,
                int concurrency,
                int outstanding,
                Duration duration) {
            return new Scenario(
                    name,
                    mode,
                    Pattern.HIT_UNIQUE,
                    batchSize,
                    concurrency,
                    outstanding,
                    0,
                    duration,
                    outstanding * 2,
                    AdmissionPolicy.REJECT,
                    Slowdown.NONE);
        }

        int configuredOutstanding() {
            return mode == Mode.DIRECT ? batchSize * databaseConcurrency : callers;
        }

        static Scenario recovery(
                String name,
                int batchSize,
                int concurrency,
                long arrivalRate,
                Duration duration,
                Slowdown slowdown) {
            return new Scenario(
                    name,
                    Mode.RECOVERY,
                    Pattern.HIT_UNIQUE,
                    batchSize,
                    concurrency,
                    0,
                    arrivalRate,
                    duration,
                    CAPACITY,
                    AdmissionPolicy.REJECT,
                    slowdown);
        }
    }

    private record PreparedBatch(List<LookupKey> logical, Set<LookupKey> unique) {}

    private record BackendSweep(
            List<DbRunResult> runs,
            List<BackendProfiler.Result> repetitions,
            List<BackendProfiler.AggregatedResult> aggregates,
            List<BackendProfiler.AggregatedResult> frontier) {}
}

record DbRunResult(
        PostgresLoadHarness.Scenario scenario,
        DatasetInfo dataset,
        MetricsSnapshot metrics,
        ResourceUsage resources,
        long startedNanos,
        double throughputRatio) {

    static DbRunResult fromProfile(
            DatasetInfo dataset, Duration duration, BackendProfiler.Result result) {
        String suffix = result.repetition() == 1 ? "" : "-r" + result.repetition();
        var scenario = PostgresLoadHarness.Scenario.direct(
                "direct-sweep-b"
                        + result.batchSize()
                        + "-c"
                        + result.backendConcurrency()
                        + suffix,
                PostgresLoadHarness.Pattern.HIT_UNIQUE,
                result.batchSize(),
                result.backendConcurrency(),
                duration);
        long logicalOperations = result.logicalOperations();
        BackendProfiler.Latency measuredLatency = result.backendLatency();
        var databaseLatency = new HistogramSnapshot(
                measuredLatency.count(),
                measuredLatency.averageNanos(),
                measuredLatency.p50(),
                measuredLatency.p95(),
                measuredLatency.p99(),
                measuredLatency.p999(),
                measuredLatency.max());
        var logicalLatency = new HistogramSnapshot(
                logicalOperations,
                measuredLatency.averageNanos(),
                measuredLatency.p50(),
                measuredLatency.p95(),
                measuredLatency.p99(),
                measuredLatency.p999(),
                measuredLatency.max());
        var batchSizes = new HistogramSnapshot(
                result.backendBatches(),
                result.batchSize(),
                result.batchSize(),
                result.batchSize(),
                result.batchSize(),
                result.batchSize(),
                result.batchSize());
        var metrics = new MetricsSnapshot(
                logicalOperations,
                logicalOperations,
                0,
                0,
                logicalOperations,
                result.successfulOperations(),
                result.missingOperations(),
                result.failedOperations(),
                result.backendBatches(),
                logicalOperations,
                logicalOperations,
                result.backendBatches(),
                logicalOperations,
                result.failedBatches(),
                0,
                0,
                result.maximumObservedConcurrency(),
                0,
                0,
                0,
                0,
                false,
                logicalLatency,
                databaseLatency,
                batchSizes,
                batchSizes,
                null);
        BackendProfiler.Resources measuredResources = result.resources();
        var resources = new ResourceUsage(
                result.elapsedNanos(),
                measuredResources.processCpuCores(),
                -1,
                measuredResources.allocatedBytes(),
                measuredResources.gcCount(),
                measuredResources.gcMillis(),
                measuredResources.startHeapBytes(),
                measuredResources.endHeapBytes(),
                Math.max(measuredResources.startHeapBytes(), measuredResources.endHeapBytes()),
                -1,
                List.of());
        return new DbRunResult(scenario, dataset, metrics, resources, 0, 0);
    }

    void validate() {
        long refused = metrics.rejected() + metrics.timedOut();
        boolean loader = scenario.mode() != PostgresLoadHarness.Mode.DIRECT;
        if (metrics.unexpected() != null) {
            throw new IllegalStateException("unexpected PostgreSQL workload failure", metrics.unexpected());
        }
        if (metrics.attempts() != metrics.admitted() + refused
                || metrics.admitted() != metrics.completed()
                || metrics.errors() != 0
                || metrics.databaseErrors() != 0
                || metrics.databaseBatches() != metrics.batches()
                || metrics.databaseKeys() != metrics.uniqueBatchKeys()
                || metrics.maxDatabaseActive() > scenario.databaseConcurrency()
                || metrics.finalDatabaseActive() != 0
                || loader
                        && (metrics.maxPending() > scenario.capacity()
                                || metrics.maxInFlight() > scenario.databaseConcurrency()
                                || metrics.finalPending() != 0
                                || metrics.finalInFlight() != 0
                                || !metrics.closed())) {
            throw new IllegalStateException("PostgreSQL workload invariants failed for " + scenario.name());
        }
    }

    DbRunResult withThroughputRatio(double ratio) {
        return new DbRunResult(scenario, dataset, metrics, resources, startedNanos, ratio);
    }

    double logicalRequestsPerSecond() {
        return metrics.completed() * 1_000_000_000d / resources.elapsedNanos();
    }

    double uniqueKeysPerSecond() {
        return metrics.databaseKeys() * 1_000_000_000d / resources.elapsedNanos();
    }

    double databaseBatchesPerSecond() {
        return metrics.databaseBatches() * 1_000_000_000d / resources.elapsedNanos();
    }

    double averageLogicalBatch() {
        return metrics.batches() == 0 ? 0 : (double) metrics.logicalBatchKeys() / metrics.batches();
    }

    double averageUniqueBatch() {
        return metrics.databaseBatches() == 0
                ? 0
                : (double) metrics.databaseKeys() / metrics.databaseBatches();
    }

    double rejectionPercent() {
        return metrics.attempts() == 0
                ? 0
                : (metrics.rejected() + metrics.timedOut()) * 100d / metrics.attempts();
    }

    static String header() {
        return "scenario,mode,pattern,dataset_rows,relation_bytes,batch_size,database_concurrency,callers,configured_outstanding,arrival_rate,duration_ns,logical_req_s,unique_keys_s,database_batches_s,throughput_ratio,attempts,admitted,completed,successful,missing,rejected,timeouts,errors,database_errors,batches,database_keys,average_logical_batch,average_unique_batch,p50_logical_batch,p95_logical_batch,p99_logical_batch,p50_unique_batch,p95_unique_batch,p99_unique_batch,max_pending,max_in_flight,max_database_concurrency,max_active_callers,database_latency_mean_ns,database_latency_p50_ns,database_latency_p95_ns,database_latency_p99_ns,logical_latency_mean_ns,logical_latency_p50_ns,logical_latency_p95_ns,logical_latency_p99_ns,process_cpu_cores,system_cpu_percent,allocated_bytes,allocated_bytes_per_attempt,gc_count,gc_millis,start_heap_bytes,end_heap_bytes,peak_heap_bytes,peak_platform_threads";
    }

    String csv() {
        long allocatedPerAttempt = metrics.attempts() == 0 || resources.allocatedBytes() < 0
                ? -1
                : resources.allocatedBytes() / metrics.attempts();
        return joinCsv(
                scenario.name(),
                scenario.mode(),
                scenario.pattern(),
                dataset.rows(),
                dataset.relationBytes(),
                scenario.batchSize(),
                scenario.databaseConcurrency(),
                scenario.callers(),
                scenario.configuredOutstanding(),
                scenario.arrivalRate(),
                scenario.duration().toNanos(),
                logicalRequestsPerSecond(),
                uniqueKeysPerSecond(),
                databaseBatchesPerSecond(),
                throughputRatio,
                metrics.attempts(),
                metrics.admitted(),
                metrics.completed(),
                metrics.successful(),
                metrics.missing(),
                metrics.rejected(),
                metrics.timedOut(),
                metrics.errors(),
                metrics.databaseErrors(),
                metrics.batches(),
                metrics.databaseKeys(),
                averageLogicalBatch(),
                averageUniqueBatch(),
                metrics.logicalBatchSizes().p50(),
                metrics.logicalBatchSizes().p95(),
                metrics.logicalBatchSizes().p99(),
                metrics.uniqueBatchSizes().p50(),
                metrics.uniqueBatchSizes().p95(),
                metrics.uniqueBatchSizes().p99(),
                metrics.maxPending(),
                metrics.maxInFlight(),
                metrics.maxDatabaseActive(),
                metrics.maxActiveCallers(),
                metrics.databaseLatency().mean(),
                metrics.databaseLatency().p50(),
                metrics.databaseLatency().p95(),
                metrics.databaseLatency().p99(),
                metrics.logicalLatency().mean(),
                metrics.logicalLatency().p50(),
                metrics.logicalLatency().p95(),
                metrics.logicalLatency().p99(),
                resources.processCpuCores(),
                resources.systemCpuPercent(),
                resources.allocatedBytes(),
                allocatedPerAttempt,
                resources.gcCount(),
                resources.gcMillis(),
                resources.startHeap(),
                resources.endHeap(),
                resources.peakHeap(),
                resources.peakPlatformThreads());
    }

    private static String joinCsv(Object... values) {
        return String.join(",", java.util.Arrays.stream(values).map(String::valueOf).toList());
    }
}
