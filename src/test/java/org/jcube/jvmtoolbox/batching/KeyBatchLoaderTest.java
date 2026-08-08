package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class KeyBatchLoaderTest {
    @Test
    void equalKeysCoalesceAndOneResultFansOutToEveryCaller() throws Exception {
        var calls = new AtomicInteger();
        var seenKeys = new AtomicReference<Set<String>>();
        try (var loader = new KeyBatchLoader<String, String>(config(3), keys -> {
            calls.incrementAndGet();
            seenKeys.set(keys);
            return Map.of(
                    "same", BatchOutcome.success("shared"),
                    "other", BatchOutcome.success("different"));
        })) {
            var first = loader.load(new String("same"));
            var duplicate = loader.load(new String("same"));
            var other = loader.load("other");

            assertEquals(Optional.of("shared"), first.get(2, SECONDS));
            assertEquals(Optional.of("shared"), duplicate.get(2, SECONDS));
            assertEquals(Optional.of("different"), other.get(2, SECONDS));
            assertEquals(1, calls.get());
            assertEquals(Set.of("same", "other"), seenKeys.get());
        }
    }

    @Test
    void equalAndDifferentKeysRemainCorrectUnderConcurrentSubmission() throws Exception {
        int submissionCount = 30;
        var calls = new AtomicInteger();
        var seenKeys = new AtomicReference<Set<String>>();
        var config = new BatchingConfig(
                submissionCount,
                Duration.ofHours(1),
                1,
                submissionCount,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
        try (var loader = new KeyBatchLoader<String, String>(config, keys -> {
                    calls.incrementAndGet();
                    seenKeys.set(keys);
                    var results = new java.util.HashMap<String, BatchOutcome<String>>();
                    keys.forEach(key -> results.put(key, BatchOutcome.success("value:" + key)));
                    return results;
                });
                var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new CountDownLatch(1);
            var results = new java.util.ArrayList<java.util.concurrent.Future<Optional<String>>>();
            for (int index = 0; index < submissionCount; index++) {
                String key = index % 2 == 0 ? new String("shared") : "unique-" + index;
                results.add(callers.submit(() -> {
                    start.await();
                    return loader.load(key).get(2, SECONDS);
                }));
            }
            start.countDown();

            for (int index = 0; index < submissionCount; index++) {
                String key = index % 2 == 0 ? "shared" : "unique-" + index;
                assertEquals(Optional.of("value:" + key), results.get(index).get(2, SECONDS));
            }
            assertEquals(1, calls.get());
            assertEquals(16, seenKeys.get().size());
        }
    }

    @Test
    void cancellingOneDuplicateDoesNotAffectAnotherCaller() throws Exception {
        var backendStarted = new CountDownLatch(1);
        var releaseBackend = new CountDownLatch(1);
        var loader = new KeyBatchLoader<String, String>(config(2), keys -> {
            backendStarted.countDown();
            releaseBackend.await();
            return Map.of("same", BatchOutcome.success("value"));
        });
        try {
            var cancelled = loader.load(new String("same"));
            var live = loader.load(new String("same"));
            assertTrue(backendStarted.await(2, SECONDS));

            cancelled.cancel(false);
            releaseBackend.countDown();

            assertThrows(CancellationException.class, () -> cancelled.get(2, SECONDS));
            assertEquals(Optional.of("value"), live.get(2, SECONDS));
        } finally {
            releaseBackend.countDown();
            loader.close();
        }
    }

    @Test
    void missingAndFailedKeysAreStructurallyDistinct() throws Exception {
        var failure = new IllegalArgumentException("key failed");
        try (var loader = new KeyBatchLoader<String, String>(config(3), keys -> Map.of(
                "found", BatchOutcome.success("value"),
                "failed", BatchOutcome.failure(failure)))) {
            var found = loader.load("found");
            var missing = loader.load("missing");
            var failed = loader.load("failed");

            assertEquals(Optional.of("value"), found.get(2, SECONDS));
            assertEquals(Optional.empty(), missing.get(2, SECONDS));
            assertSame(failure, assertThrows(ExecutionException.class, () -> failed.get(2, SECONDS)).getCause());
        }
    }

    @Test
    void backendFailureFailsEveryLiveKeyInTheBatch() throws Exception {
        var failure = new IOException("backend failed");
        try (var loader = new KeyBatchLoader<String, String>(config(2), keys -> {
            throw failure;
        })) {
            var first = loader.load("first");
            var second = loader.load("second");

            assertSame(failure, assertThrows(ExecutionException.class, () -> first.get(2, SECONDS)).getCause());
            assertSame(failure, assertThrows(ExecutionException.class, () -> second.get(2, SECONDS)).getCause());
        }
    }

    @Test
    void laterBatchingWindowsDoNotReuseResults() throws Exception {
        var calls = new AtomicInteger();
        try (var loader = new KeyBatchLoader<String, String>(config(1), keys -> {
            int call = calls.incrementAndGet();
            return Map.of("key", BatchOutcome.success("value-" + call));
        })) {
            assertEquals(Optional.of("value-1"), loader.load("key").get(2, SECONDS));
            assertEquals(Optional.of("value-2"), loader.load("key").get(2, SECONDS));
            assertEquals(2, calls.get());
        }
    }

    @Test
    void unrequestedBackendKeysFailTheWholeBatch() throws Exception {
        try (var loader = new KeyBatchLoader<String, String>(config(1), keys ->
                Map.of("other", BatchOutcome.success("value")))) {
            var failure = assertThrows(ExecutionException.class, () -> loader.load("requested").get(2, SECONDS));

            assertEquals(IllegalStateException.class, failure.getCause().getClass());
        }
    }

    @Test
    void nullBackendValuesAreFailuresRatherThanMissing() throws Exception {
        try (var loader = new KeyBatchLoader<String, String>(config(1), keys ->
                Map.of("key", BatchOutcome.success(null)))) {
            var failure = assertThrows(ExecutionException.class, () -> loader.load("key").get(2, SECONDS));

            assertEquals(IllegalStateException.class, failure.getCause().getClass());
        }
    }

    private static BatchingConfig config(int maxBatchSize) {
        return new BatchingConfig(
                maxBatchSize,
                Duration.ofSeconds(1),
                2,
                8,
                AdmissionPolicy.REJECT,
                Duration.ZERO);
    }
}
