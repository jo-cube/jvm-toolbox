package org.jcube.jvmtoolbox.perf;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
import org.jcube.jvmtoolbox.batching.BatchingConfig;
import org.jcube.jvmtoolbox.batching.KeyBatchLoader;
import org.jcube.jvmtoolbox.batching.MicroBatcher;

public final class BatchingLoadHarness {
    private static final Path REPORT_DIRECTORY = Path.of("build", "reports", "perf");
    private static final Duration LATENCY = Duration.ofMillis(2);
    private static final Duration JITTER = Duration.ofNanos(250_000);
    private static final Duration MAX_WAIT = Duration.ofNanos(500_000);
    private static final Duration ADMISSION_TIMEOUT = Duration.ofMillis(1);
    private static final int BATCH_SIZE = 128;
    private static final int MAX_CONCURRENT_BATCHES = 8;
    private static final int DEFAULT_CAPACITY = 65_536;

    private BatchingLoadHarness() {}

    public static void main(String[] args) throws Exception {
        String profile = args.length == 0 ? "quick" : args[0];
        List<PerfScenario> scenarios = scenarios(profile, Arrays.copyOfRange(args, 1, args.length));
        Files.createDirectories(REPORT_DIRECTORY);

        if (!profile.equals("custom")) {
            System.out.println("warming virtual-thread and batching paths...");
            runScenario(scenario(
                    "warmup",
                    PerfScenario.Target.MICRO,
                    PerfScenario.Mode.CLOSED,
                    1_000,
                    0,
                    Duration.ofMillis(750),
                    AdmissionPolicy.WAIT,
                    DEFAULT_CAPACITY,
                    normalBackend(),
                    64));
        }

        var results = new ArrayList<RunResult>(scenarios.size());
        for (PerfScenario scenario : scenarios) {
            System.out.printf(Locale.ROOT, "running %-26s", scenario.name());
            RunResult result = runScenario(scenario);
            result.validate();
            results.add(result);
            print(result);
        }
        write(profile, results);
    }

    private static RunResult runScenario(PerfScenario scenario) throws Exception {
        System.gc();
        var metrics = new LoadMetrics(scenario);
        var backend = new SyntheticBackend(scenario, metrics);
        var probe = new ResourceProbe(metrics);
        var config = new BatchingConfig(
                scenario.batchSize(),
                scenario.maxWait(),
                scenario.maxConcurrentBatches(),
                scenario.capacity(),
                scenario.admissionPolicy(),
                scenario.admissionTimeout());

        long endNanos;
        if (scenario.target() == PerfScenario.Target.MICRO) {
            try (var batcher = new MicroBatcher<Request, Long>(config, backend::process, metrics)) {
                runWorkload(
                        scenario,
                        metrics,
                        probe,
                        (id, scheduled) -> executeMicro(batcher, backend, metrics, id, scheduled));
            }
            endNanos = System.nanoTime();
        } else {
            try (var loader = new KeyBatchLoader<KeyRequest, Integer>(config, backend::load, metrics)) {
                runWorkload(
                        scenario,
                        metrics,
                        probe,
                        (id, scheduled) -> executeKeyed(loader, backend, metrics, scenario, id, scheduled));
            }
            endNanos = System.nanoTime();
        }

        ResourceUsage resources = probe.finish(endNanos);
        return new RunResult(scenario, metrics.snapshot(), resources, backend.maximumActive(), backend.concurrencyViolations());
    }

    private static void runWorkload(
            PerfScenario scenario,
            LoadMetrics metrics,
            ResourceProbe probe,
            Operation operation)
            throws Exception {
        if (scenario.mode() == PerfScenario.Mode.CLOSED) {
            runClosedLoop(scenario, metrics, probe, operation);
        } else {
            runOpenLoop(scenario, metrics, probe, operation);
        }
    }

    private static void runClosedLoop(
            PerfScenario scenario,
            LoadMetrics metrics,
            ResourceProbe probe,
            Operation operation)
            throws Exception {
        var ready = new CountDownLatch(scenario.callers());
        var startGate = new CountDownLatch(1);
        var done = new CountDownLatch(scenario.callers());
        var ids = new AtomicLong();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int caller = 0; caller < scenario.callers(); caller++) {
                executor.execute(() -> {
                    ready.countDown();
                    try {
                        startGate.await();
                        metrics.callerStarted();
                        long end = metrics.endNanos(scenario.duration());
                        while (System.nanoTime() < end) {
                            long now = System.nanoTime();
                            operation.execute(ids.getAndIncrement(), now);
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
            long start = System.nanoTime();
            metrics.start(start);
            probe.start(start);
            startGate.countDown();
            done.await();
        }
    }

    private static void runOpenLoop(
            PerfScenario scenario,
            LoadMetrics metrics,
            ResourceProbe probe,
            Operation operation) {
        long requestCount = Math.multiplyExact(scenario.arrivalRate(), scenario.duration().toNanos())
                / 1_000_000_000L;
        long start = System.nanoTime() + 10_000_000L;
        metrics.start(start);
        probe.start(start);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (long sequence = 0; sequence < requestCount; sequence++) {
                long scheduled = start + sequence * 1_000_000_000L / scenario.arrivalRate();
                waitUntil(scheduled);
                metrics.recordLaunchLag(System.nanoTime() - scheduled);
                long id = sequence;
                executor.execute(() -> {
                    metrics.callerStarted();
                    try {
                        operation.execute(id, scheduled);
                    } catch (Throwable failure) {
                        metrics.unexpected(failure);
                    } finally {
                        metrics.callerFinished();
                    }
                });
            }
        }
    }

    private static void executeMicro(
            MicroBatcher<Request, Long> batcher,
            SyntheticBackend backend,
            LoadMetrics metrics,
            long id,
            long scheduledNanos)
            throws InterruptedException {
        metrics.attempt();
        long submitNanos = System.nanoTime();
        var request = new Request(id, scheduledNanos, submitNanos);
        CompletableFuture<Long> future;
        metrics.admissionStarted();
        try {
            future = batcher.submit(request);
        } catch (RejectedExecutionException | TimeoutException expected) {
            return;
        } finally {
            metrics.admissionFinished(System.nanoTime() - submitNanos);
        }
        try {
            if (future.join() != id) {
                throw new IllegalStateException("micro-batcher result correlation failed");
            }
            metrics.completed(scheduledNanos, LoadMetrics.Completion.SUCCESS);
        } catch (CompletionException failure) {
            if (!backend.expected(failure.getCause())) {
                throw failure;
            }
            metrics.completed(scheduledNanos, LoadMetrics.Completion.FAILURE);
        }
    }

    private static void executeKeyed(
            KeyBatchLoader<KeyRequest, Integer> loader,
            SyntheticBackend backend,
            LoadMetrics metrics,
            PerfScenario scenario,
            long id,
            long scheduledNanos)
            throws InterruptedException {
        metrics.attempt();
        long submitNanos = System.nanoTime();
        int key = Math.floorMod(id, scenario.keySpace());
        var request = new KeyRequest(id, key, scheduledNanos, submitNanos);
        CompletableFuture<Optional<Integer>> future;
        metrics.admissionStarted();
        try {
            future = loader.load(request);
        } catch (RejectedExecutionException | TimeoutException expected) {
            return;
        } finally {
            metrics.admissionFinished(System.nanoTime() - submitNanos);
        }
        try {
            Optional<Integer> result = future.join();
            if (result.isEmpty()) {
                metrics.completed(scheduledNanos, LoadMetrics.Completion.MISSING);
            } else if (result.orElseThrow() == key) {
                metrics.completed(scheduledNanos, LoadMetrics.Completion.SUCCESS);
            } else {
                throw new IllegalStateException("keyed result correlation failed");
            }
        } catch (CompletionException failure) {
            if (!backend.expected(failure.getCause())) {
                throw failure;
            }
            metrics.completed(scheduledNanos, LoadMetrics.Completion.FAILURE);
        }
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

    private static List<PerfScenario> scenarios(String profile, String[] arguments) {
        return switch (profile) {
            case "quick" -> quickScenarios();
            case "full" -> fullScenarios();
            case "profile" -> List.of(scenario(
                    "profile-closed-25k",
                    PerfScenario.Target.MICRO,
                    PerfScenario.Mode.CLOSED,
                    25_000,
                    0,
                    Duration.ofSeconds(5),
                    AdmissionPolicy.WAIT,
                    DEFAULT_CAPACITY,
                    normalBackend(),
                    64));
            case "custom" -> List.of(customScenario(arguments));
            default -> throw new IllegalArgumentException("unknown profile: " + profile);
        };
    }

    private static List<PerfScenario> quickScenarios() {
        BackendModel normal = normalBackend();
        return List.of(
                scenario("closed-1k", PerfScenario.Target.MICRO, PerfScenario.Mode.CLOSED, 1_000, 0, Duration.ofSeconds(1), AdmissionPolicy.WAIT, DEFAULT_CAPACITY, normal, 64),
                scenario("closed-10k", PerfScenario.Target.MICRO, PerfScenario.Mode.CLOSED, 10_000, 0, Duration.ofSeconds(1), AdmissionPolicy.WAIT, DEFAULT_CAPACITY, normal, 64),
                scenario("open-near", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 450_000, Duration.ofSeconds(1), AdmissionPolicy.REJECT, 32_768, normal, 64),
                scenario("open-above-reject", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 650_000, Duration.ofSeconds(1), AdmissionPolicy.REJECT, 8_192, normal, 64),
                scenario("slowdown-recovery", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 350_000, Duration.ofSeconds(3), AdmissionPolicy.REJECT, 32_768, slowdownBackend(Duration.ofSeconds(1), Duration.ofSeconds(1)), 64),
                scenario("keyed-10k", PerfScenario.Target.KEYED, PerfScenario.Mode.CLOSED, 10_000, 0, Duration.ofSeconds(1), AdmissionPolicy.WAIT, DEFAULT_CAPACITY, keyedBackend(), 64));
    }

    private static List<PerfScenario> fullScenarios() {
        BackendModel normal = normalBackend();
        var scenarios = new ArrayList<PerfScenario>();
        for (int callers : List.of(1_000, 5_000, 10_000, 25_000, 50_000)) {
            scenarios.add(scenario(
                    "closed-" + callers,
                    PerfScenario.Target.MICRO,
                    PerfScenario.Mode.CLOSED,
                    callers,
                    0,
                    Duration.ofSeconds(2),
                    AdmissionPolicy.WAIT,
                    DEFAULT_CAPACITY,
                    normal,
                    64));
        }
        scenarios.add(scenario("open-below", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 250_000, Duration.ofSeconds(3), AdmissionPolicy.REJECT, 32_768, normal, 64));
        scenarios.add(scenario("open-near", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 450_000, Duration.ofSeconds(3), AdmissionPolicy.REJECT, 32_768, normal, 64));
        scenarios.add(scenario("open-above-reject", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 650_000, Duration.ofSeconds(2), AdmissionPolicy.REJECT, 8_192, normal, 64));
        scenarios.add(scenario("closed-overload-wait", PerfScenario.Target.MICRO, PerfScenario.Mode.CLOSED, 50_000, 0, Duration.ofSeconds(2), AdmissionPolicy.WAIT, 8_192, normal, 64));
        scenarios.add(scenario("closed-overload-timeout", PerfScenario.Target.MICRO, PerfScenario.Mode.CLOSED, 50_000, 0, Duration.ofSeconds(2), AdmissionPolicy.WAIT_WITH_TIMEOUT, 8_192, normal, 64));
        scenarios.add(scenario("slowdown-recovery", PerfScenario.Target.MICRO, PerfScenario.Mode.OPEN, 0, 350_000, Duration.ofSeconds(9), AdmissionPolicy.REJECT, 32_768, slowdownBackend(Duration.ofSeconds(3), Duration.ofSeconds(3)), 64));
        scenarios.add(scenario("keyed-10k", PerfScenario.Target.KEYED, PerfScenario.Mode.CLOSED, 10_000, 0, Duration.ofSeconds(3), AdmissionPolicy.WAIT, DEFAULT_CAPACITY, keyedBackend(), 64));
        return List.copyOf(scenarios);
    }

    private static PerfScenario customScenario(String[] arguments) {
        Map<String, String> values = new HashMap<>();
        for (String argument : arguments) {
            String[] parts = argument.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("custom arguments must be key=value: " + argument);
            }
            values.put(parts[0], parts[1]);
        }
        var target = enumValue(PerfScenario.Target.class, values.getOrDefault("target", "micro"));
        var mode = enumValue(PerfScenario.Mode.class, values.getOrDefault("mode", "closed"));
        var policy = enumValue(AdmissionPolicy.class, values.getOrDefault("policy", "wait"));
        var backend = new BackendModel(
                duration(values, "latency", "2ms"),
                duration(values, "jitter", "250us"),
                duration(values, "slowdownStart", "0s"),
                duration(values, "slowdownDuration", "0s"),
                Double.parseDouble(values.getOrDefault("slowdownFactor", "1")),
                integer(values, "missingEvery", 0),
                integer(values, "itemFailureEvery", 0),
                integer(values, "batchFailureEvery", 0));
        return new PerfScenario(
                values.getOrDefault("name", "custom"),
                target,
                mode,
                integer(values, "callers", 10_000),
                longValue(values, "rate", 300_000),
                duration(values, "duration", "3s"),
                integer(values, "batch", BATCH_SIZE),
                duration(values, "wait", "500us"),
                integer(values, "concurrent", MAX_CONCURRENT_BATCHES),
                integer(values, "capacity", DEFAULT_CAPACITY),
                policy,
                duration(values, "timeout", "1ms"),
                backend,
                integer(values, "keySpace", 64));
    }

    private static PerfScenario scenario(
            String name,
            PerfScenario.Target target,
            PerfScenario.Mode mode,
            int callers,
            long arrivalRate,
            Duration duration,
            AdmissionPolicy policy,
            int capacity,
            BackendModel backend,
            int keySpace) {
        return new PerfScenario(
                name,
                target,
                mode,
                callers,
                arrivalRate,
                duration,
                BATCH_SIZE,
                MAX_WAIT,
                MAX_CONCURRENT_BATCHES,
                capacity,
                policy,
                ADMISSION_TIMEOUT,
                backend,
                keySpace);
    }

    private static BackendModel normalBackend() {
        return new BackendModel(LATENCY, JITTER, Duration.ZERO, Duration.ZERO, 1, 0, 0, 0);
    }

    private static BackendModel slowdownBackend(Duration start, Duration duration) {
        return new BackendModel(LATENCY, JITTER, start, duration, 4, 0, 0, 0);
    }

    private static BackendModel keyedBackend() {
        return new BackendModel(LATENCY, JITTER, Duration.ZERO, Duration.ZERO, 1, 100, 200, 0);
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, String value) {
        return Enum.valueOf(type, value.toUpperCase(Locale.ROOT));
    }

    private static int integer(Map<String, String> values, String key, int fallback) {
        return Integer.parseInt(values.getOrDefault(key, Integer.toString(fallback)));
    }

    private static long longValue(Map<String, String> values, String key, long fallback) {
        return Long.parseLong(values.getOrDefault(key, Long.toString(fallback)));
    }

    private static Duration duration(Map<String, String> values, String key, String fallback) {
        return parseDuration(values.getOrDefault(key, fallback));
    }

    private static Duration parseDuration(String text) {
        if (text.endsWith("ns")) {
            return Duration.ofNanos(Long.parseLong(text.substring(0, text.length() - 2)));
        }
        if (text.endsWith("us")) {
            return Duration.ofNanos(Long.parseLong(text.substring(0, text.length() - 2)) * 1_000);
        }
        if (text.endsWith("ms")) {
            return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2)));
        }
        if (text.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1)));
        }
        throw new IllegalArgumentException("duration requires ns/us/ms/s suffix: " + text);
    }

    private static void print(RunResult result) {
        MetricsSnapshot metrics = result.metrics();
        System.out.printf(
                Locale.ROOT,
                " %8.0f req/s %5.1f%% ceiling batch=%5.1f (%4.1f%%) p99=%7.1fms queue=%6.1fus reject=%6.2f%% cpu=%4.2f cores heap=%5.1fMB%n",
                result.requestsPerSecond(),
                result.capacityPercent(),
                result.averageBatchSize(),
                result.fillPercent(),
                nanosToMillis(metrics.endToEnd().p99()),
                nanosToMicros(metrics.queueDelay().p99()),
                result.rejectionPercent(),
                result.resources().cpuCores(),
                bytesToMegabytes(result.resources().peakHeapBytes()));
        if (result.scenario().hasSlowdown()) {
            System.out.printf(
                    Locale.ROOT,
                    "  phases: before=%8.0f/s slow=%8.0f/s recovery=%8.0f/s; p99=%5.1f/%5.1f/%5.1fms%n",
                    result.preRequestsPerSecond(),
                    result.slowRequestsPerSecond(),
                    result.recoveryRequestsPerSecond(),
                    nanosToMillis(metrics.preLatency().p99()),
                    nanosToMillis(metrics.slowLatency().p99()),
                    nanosToMillis(metrics.recoveryLatency().p99()));
        }
    }

    private static void write(String profile, List<RunResult> results) throws IOException {
        Path resultsFile = REPORT_DIRECTORY.resolve(profile + "-results.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(resultsFile)) {
            writer.write(RunResult.header());
            writer.newLine();
            for (RunResult result : results) {
                writer.write(result.csv());
                writer.newLine();
            }
        }

        Path samplesFile = REPORT_DIRECTORY.resolve(profile + "-samples.csv");
        try (BufferedWriter writer = Files.newBufferedWriter(samplesFile)) {
            writer.write("scenario,seconds,attempts,completed,completed_per_second,rejected,timeouts,pending,in_flight,active_callers,admission_callers,heap_bytes,platform_threads");
            writer.newLine();
            for (RunResult result : results) {
                MetricSample previous = null;
                for (MetricSample sample : result.resources().samples()) {
                    double rate = previous == null || sample.seconds() == previous.seconds()
                            ? 0
                            : (sample.completed() - previous.completed())
                                    / (sample.seconds() - previous.seconds());
                    writer.write(String.format(
                            Locale.ROOT,
                            "%s,%.3f,%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d",
                            result.scenario().name(),
                            sample.seconds(),
                            sample.attempts(),
                            sample.completed(),
                            rate,
                            sample.rejected(),
                            sample.timedOut(),
                            sample.pending(),
                            sample.inFlight(),
                            sample.activeCallers(),
                            sample.admissionCallers(),
                            sample.heapBytes(),
                            sample.platformThreads()));
                    writer.newLine();
                    previous = sample;
                }
            }
        }
        System.out.println("reports: " + resultsFile + " and " + samplesFile);
    }

    private static double nanosToMicros(double nanos) {
        return nanos / 1_000d;
    }

    private static double nanosToMillis(double nanos) {
        return nanos / 1_000_000d;
    }

    private static double bytesToMegabytes(long bytes) {
        return bytes / (1024d * 1024d);
    }

    @FunctionalInterface
    private interface Operation {
        void execute(long id, long scheduledNanos) throws Exception;
    }
}

record RunResult(
        PerfScenario scenario,
        MetricsSnapshot metrics,
        ResourceUsage resources,
        int maximumBackendConcurrency,
        int concurrencyViolations) {

    void validate() {
        long rejected = metrics.capacityRejected() + metrics.timedOut() + metrics.closedRejected();
        long maximumObservedPending = scenario.capacity()
                + (long) scenario.batchSize() * scenario.maxConcurrentBatches();
        if (metrics.unexpected() != null) {
            throw new IllegalStateException("unexpected workload failure", metrics.unexpected());
        }
        if (concurrencyViolations != 0
                || maximumBackendConcurrency > scenario.maxConcurrentBatches()
                || metrics.maxInFlight() > scenario.maxConcurrentBatches()
                || metrics.maxPending() > maximumObservedPending
                || metrics.finalPending() != 0
                || metrics.finalInFlight() != 0
                || !metrics.closed()
                || metrics.attempts() != metrics.admitted() + rejected
                || metrics.admitted() != metrics.dispatchedRequests()
                || metrics.admitted() != metrics.completed()
                || metrics.batches() != metrics.sizeTriggered() + metrics.timeTriggered()) {
            throw new IllegalStateException(
                    "workload invariants failed for " + scenario.name() + ": " + metrics);
        }
    }

    double requestsPerSecond() {
        return metrics.completed() * 1_000_000_000d / resources.elapsedNanos();
    }

    double capacityPercent() {
        return requestsPerSecond() * 100 / scenario.theoreticalRequestsPerSecond();
    }

    double observedBackendCeiling() {
        double backendMeanNanos = metrics.backendLatency().mean();
        return backendMeanNanos == 0
                ? 0
                : scenario.maxConcurrentBatches() * scenario.batchSize() * 1_000_000_000d
                        / backendMeanNanos;
    }

    double observedCapacityPercent() {
        double ceiling = observedBackendCeiling();
        return ceiling == 0 ? 0 : requestsPerSecond() * 100 / ceiling;
    }

    double averageBatchSize() {
        return metrics.batches() == 0 ? 0 : (double) metrics.dispatchedRequests() / metrics.batches();
    }

    double fillPercent() {
        return averageBatchSize() * 100 / scenario.batchSize();
    }

    double averageUniqueKeys() {
        return metrics.batches() == 0 ? 0 : (double) metrics.uniqueKeys() / metrics.batches();
    }

    double coalescedPercent() {
        return metrics.keyedRequests() == 0
                ? 0
                : (metrics.keyedRequests() - metrics.uniqueKeys()) * 100d / metrics.keyedRequests();
    }

    double rejectionPercent() {
        long rejected = metrics.capacityRejected() + metrics.timedOut() + metrics.closedRejected();
        return metrics.attempts() == 0 ? 0 : rejected * 100d / metrics.attempts();
    }

    double preRequestsPerSecond() {
        return phaseRate(metrics.preCompleted(), scenario.backend().slowdownStart());
    }

    double slowRequestsPerSecond() {
        return phaseRate(metrics.slowCompleted(), scenario.backend().slowdownDuration());
    }

    double recoveryRequestsPerSecond() {
        long slowdownEnd = scenario.backend().slowdownStart()
                .plus(scenario.backend().slowdownDuration())
                .toNanos();
        long recoveryNanos = Math.max(0, resources.elapsedNanos() - slowdownEnd);
        return recoveryNanos == 0
                ? 0
                : metrics.recoveryCompleted() * 1_000_000_000d / recoveryNanos;
    }

    private static double phaseRate(long completed, Duration duration) {
        return duration.isZero() ? 0 : completed * 1_000_000_000d / duration.toNanos();
    }

    static String header() {
        return "scenario,target,mode,callers,arrival_rate,policy,batch_size,max_wait_ns,max_concurrent,capacity,backend_latency_ns,theoretical_req_s,backend_mean_ns,observed_ceiling_req_s,completed_req_s,capacity_percent,observed_capacity_percent,attempts,admitted,completed,successful,missing,failed,capacity_rejected,timeouts,max_pending,max_in_flight,max_backend_concurrency,max_active_callers,max_admission_callers,batches,average_batch,p50_batch,p95_batch,fill_percent,size_triggered,time_triggered,keyed_requests,unique_keys,average_unique_keys,coalesced_percent,e2e_p50_ns,e2e_p95_ns,e2e_p99_ns,e2e_p999_ns,admission_p99_ns,queue_p99_ns,backend_p99_ns,launch_lag_p99_ns,cpu_cores,cpu_percent,allocated_bytes,allocated_bytes_per_attempt,gc_count,gc_millis,baseline_heap_bytes,waiting_heap_bytes,peak_heap_bytes,peak_heap_delta_bytes,peak_platform_threads,pre_req_s,slow_req_s,recovery_req_s,pre_p99_ns,slow_p99_ns,recovery_p99_ns";
    }

    String csv() {
        long allocatedPerAttempt = metrics.attempts() == 0 || resources.allocatedBytes() < 0
                ? -1
                : resources.allocatedBytes() / metrics.attempts();
        return joinCsv(
                scenario.name(),
                scenario.target(),
                scenario.mode(),
                scenario.callers(),
                scenario.arrivalRate(),
                scenario.admissionPolicy(),
                scenario.batchSize(),
                scenario.maxWait().toNanos(),
                scenario.maxConcurrentBatches(),
                scenario.capacity(),
                scenario.backend().latency().toNanos(),
                scenario.theoreticalRequestsPerSecond(),
                metrics.backendLatency().mean(),
                observedBackendCeiling(),
                requestsPerSecond(),
                capacityPercent(),
                observedCapacityPercent(),
                metrics.attempts(),
                metrics.admitted(),
                metrics.completed(),
                metrics.successful(),
                metrics.missing(),
                metrics.failed(),
                metrics.capacityRejected(),
                metrics.timedOut(),
                metrics.maxPending(),
                metrics.maxInFlight(),
                maximumBackendConcurrency,
                metrics.maxActiveCallers(),
                metrics.maxAdmissionCallers(),
                metrics.batches(),
                averageBatchSize(),
                metrics.batchSizes().p50(),
                metrics.batchSizes().p95(),
                fillPercent(),
                metrics.sizeTriggered(),
                metrics.timeTriggered(),
                metrics.keyedRequests(),
                metrics.uniqueKeys(),
                averageUniqueKeys(),
                coalescedPercent(),
                metrics.endToEnd().p50(),
                metrics.endToEnd().p95(),
                metrics.endToEnd().p99(),
                metrics.endToEnd().p999(),
                metrics.admissionDelay().p99(),
                metrics.queueDelay().p99(),
                metrics.backendLatency().p99(),
                metrics.launchLag().p99(),
                resources.cpuCores(),
                resources.cpuPercent(),
                resources.allocatedBytes(),
                allocatedPerAttempt,
                resources.gcCount(),
                resources.gcMillis(),
                resources.baselineHeapBytes(),
                resources.startHeapBytes() - resources.baselineHeapBytes(),
                resources.peakHeapBytes(),
                resources.peakHeapBytes() - resources.baselineHeapBytes(),
                resources.peakPlatformThreads(),
                preRequestsPerSecond(),
                slowRequestsPerSecond(),
                recoveryRequestsPerSecond(),
                metrics.preLatency().p99(),
                metrics.slowLatency().p99(),
                metrics.recoveryLatency().p99());
    }

    private static String joinCsv(Object... values) {
        return String.join(",", Arrays.stream(values).map(String::valueOf).toList());
    }
}
