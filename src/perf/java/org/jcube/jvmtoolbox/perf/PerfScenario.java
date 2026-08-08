package org.jcube.jvmtoolbox.perf;

import java.time.Duration;
import java.util.Objects;
import org.jcube.jvmtoolbox.batching.AdmissionPolicy;

record PerfScenario(
        String name,
        Target target,
        Mode mode,
        int callers,
        long arrivalRate,
        Duration duration,
        int batchSize,
        Duration maxWait,
        int maxConcurrentBatches,
        int capacity,
        AdmissionPolicy admissionPolicy,
        Duration admissionTimeout,
        BackendModel backend,
        int keySpace) {

    enum Target {
        MICRO,
        KEYED
    }

    enum Mode {
        CLOSED,
        OPEN
    }

    PerfScenario {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(duration, "duration");
        Objects.requireNonNull(maxWait, "maxWait");
        Objects.requireNonNull(admissionPolicy, "admissionPolicy");
        Objects.requireNonNull(admissionTimeout, "admissionTimeout");
        Objects.requireNonNull(backend, "backend");
        if (name.isBlank()
                || callers < 0
                || arrivalRate < 0
                || duration.isZero()
                || duration.isNegative()
                || batchSize < 1
                || maxWait.isNegative()
                || maxConcurrentBatches < 1
                || capacity < 1
                || keySpace < 1) {
            throw new IllegalArgumentException("invalid performance scenario");
        }
        if (mode == Mode.CLOSED && callers < 1) {
            throw new IllegalArgumentException("closed-loop workloads need callers");
        }
        if (mode == Mode.OPEN && arrivalRate < 1) {
            throw new IllegalArgumentException("open-loop workloads need an arrival rate");
        }
    }

    double theoreticalRequestsPerSecond() {
        return (double) maxConcurrentBatches * batchSize * 1_000_000_000d
                / backend.latency().toNanos();
    }

    boolean hasSlowdown() {
        return !backend.slowdownDuration().isZero();
    }

    int phase(long nowNanos, long startNanos) {
        if (!hasSlowdown()) {
            return 0;
        }
        long elapsed = nowNanos - startNanos;
        if (elapsed < backend.slowdownStart().toNanos()) {
            return 0;
        }
        if (elapsed < backend.slowdownStart().plus(backend.slowdownDuration()).toNanos()) {
            return 1;
        }
        return 2;
    }
}

record BackendModel(
        Duration latency,
        Duration jitter,
        Duration slowdownStart,
        Duration slowdownDuration,
        double slowdownFactor,
        int missingEvery,
        int itemFailureEvery,
        int batchFailureEvery) {

    BackendModel {
        Objects.requireNonNull(latency, "latency");
        Objects.requireNonNull(jitter, "jitter");
        Objects.requireNonNull(slowdownStart, "slowdownStart");
        Objects.requireNonNull(slowdownDuration, "slowdownDuration");
        if (latency.isZero()
                || latency.isNegative()
                || jitter.isNegative()
                || jitter.compareTo(latency) >= 0
                || slowdownStart.isNegative()
                || slowdownDuration.isNegative()
                || slowdownFactor < 1
                || missingEvery < 0
                || itemFailureEvery < 0
                || batchFailureEvery < 0) {
            throw new IllegalArgumentException("invalid synthetic backend model");
        }
    }
}
