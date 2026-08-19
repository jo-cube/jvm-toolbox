package org.jcube.jvmtoolbox.batching;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class WindowedAccumulatorTest {
    private static final long MAX_WAIT = Duration.ofHours(1).toNanos();

    @Test
    void countTriggerRotatesAndTransfersDistinctAccumulatorStates() throws Exception {
        var created = new AtomicInteger();
        var processed = new LinkedBlockingQueue<long[]>();
        try (var windows = new WindowedAccumulator<Integer, long[]>(
                config(3, Duration.ofHours(1), 8, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> {
                    created.incrementAndGet();
                    return new long[1];
                },
                (sum, input) -> sum[0] += input,
                processed::add)) {
            windows.add(1);
            windows.add(2);
            windows.add(3);
            long[] first = processed.poll(2, SECONDS);

            windows.add(4);
            windows.add(5);
            windows.flush();
            long[] second = processed.poll(2, SECONDS);

            assertEquals(6, first[0]);
            assertEquals(9, second[0]);
            assertNotSame(first, second);
            assertEquals(2, created.get());
        }
    }

    @Test
    void accumulationRunsOnTheAdmittingCaller() throws Exception {
        var callbackThread = new AtomicReference<Thread>();
        try (var windows = new WindowedAccumulator<Integer, int[]>(
                config(1, Duration.ZERO, 1, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> callbackThread.set(Thread.currentThread()),
                ignored -> {})) {
            Thread caller = Thread.currentThread();

            windows.add(1);

            assertSame(caller, callbackThread.get());
        }
    }

    @Test
    void oldestContributionStartsTheTimeWindow() throws Exception {
        var clock = new AtomicLong();
        var processed = new LinkedBlockingQueue<List<Integer>>();
        try (var windows = listAccumulator(3, clock, processed)) {
            windows.add(1);
            clock.set(MAX_WAIT / 2);
            windows.add(2);

            clock.set(MAX_WAIT - 1);
            windows.signalCoordinator();
            assertNull(processed.poll(50, MILLISECONDS));

            clock.set(MAX_WAIT);
            windows.signalCoordinator();
            assertEquals(List.of(1, 2), processed.poll(2, SECONDS));
        }
    }

    @RepeatedTest(20)
    void contributionAtTheDeadlineStartsTheNextWindowEvenDuringATimerRace() throws Exception {
        var clock = new AtomicLong();
        var processed = new LinkedBlockingQueue<List<Integer>>();
        try (var windows = listAccumulator(3, clock, processed);
                var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            windows.add(1);
            clock.set(MAX_WAIT);
            var add = callers.submit(() -> {
                windows.add(2);
                return null;
            });
            var timer = callers.submit(windows::signalCoordinator);
            add.get(2, SECONDS);
            timer.get(2, SECONDS);
            windows.flush();

            assertEquals(List.of(1), processed.poll(2, SECONDS));
            assertEquals(List.of(2), processed.poll(2, SECONDS));
        }
    }

    @Test
    void realClockWakesAtTheDeadline() throws Exception {
        var processed = new LinkedBlockingQueue<Integer>();
        try (var windows = new WindowedAccumulator<Integer, int[]>(
                config(2, Duration.ofMillis(20), 2, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                sum -> processed.add(sum[0]))) {
            windows.add(1);

            assertEquals(1, processed.poll(2, SECONDS));
        }
    }

    @RepeatedTest(5)
    void concurrentContributionsAreIncludedExactlyOnce() throws Exception {
        var processed = new ArrayList<List<Integer>>();
        try (var windows = new WindowedAccumulator<Integer, List<Integer>>(
                        config(17, Duration.ofHours(1), 1_000, AdmissionPolicy.REJECT, Duration.ZERO),
                        ArrayList::new,
                        List::add,
                        window -> {
                            synchronized (processed) {
                                processed.add(List.copyOf(window));
                            }
                        });
                var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var additions = new ArrayList<java.util.concurrent.Future<?>>();
            for (int input = 0; input < 1_000; input++) {
                int value = input;
                additions.add(callers.submit(() -> {
                    windows.add(value);
                    return null;
                }));
            }
            for (var addition : additions) {
                addition.get(2, SECONDS);
            }
            windows.flush();

            List<Integer> flattened;
            synchronized (processed) {
                assertTrue(processed.stream().allMatch(window -> window.size() <= 17));
                flattened = processed.stream().flatMap(List::stream).sorted().toList();
            }
            assertEquals(1_000, flattened.size());
            for (int expected = 0; expected < flattened.size(); expected++) {
                assertEquals(expected, flattened.get(expected));
            }
        }
    }

    @Test
    void processingIsOrderedWhileTheNextWindowForms() throws Exception {
        var firstStarted = new CountDownLatch(1);
        var secondStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var activeProcessors = new AtomicInteger();
        var maximumProcessors = new AtomicInteger();
        var processed = new ArrayList<Integer>();
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(2, Duration.ofHours(1), 4, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                sum -> {
                    int active = activeProcessors.incrementAndGet();
                    maximumProcessors.accumulateAndGet(active, Math::max);
                    if (sum[0] == 3) {
                        firstStarted.countDown();
                        releaseFirst.await();
                    } else {
                        secondStarted.countDown();
                    }
                    processed.add(sum[0]);
                    activeProcessors.decrementAndGet();
                });
        try {
            windows.add(1);
            windows.add(2);
            assertTrue(firstStarted.await(2, SECONDS));

            windows.add(3);
            windows.add(4);
            assertFalse(secondStarted.await(50, MILLISECONDS));
            releaseFirst.countDown();
            windows.flush();

            assertEquals(List.of(3, 7), processed);
            assertEquals(1, maximumProcessors.get());
        } finally {
            releaseFirst.countDown();
            windows.close();
        }
    }

    @Test
    void timeBoundaryCompletesTheNextWindowWhileProcessingIsBusy() throws Exception {
        var clock = new AtomicLong();
        var firstStarted = new CountDownLatch(1);
        var secondProcessed = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var processed = new ArrayList<List<Integer>>();
        var windows = new WindowedAccumulator<Integer, List<Integer>>(
                config(2, Duration.ofHours(1), 3, AdmissionPolicy.REJECT, Duration.ZERO),
                ArrayList::new,
                List::add,
                window -> {
                    if (calls.getAndIncrement() == 0) {
                        firstStarted.countDown();
                        releaseFirst.await();
                    }
                    processed.add(List.copyOf(window));
                    if (processed.size() == 2) {
                        secondProcessed.countDown();
                    }
                },
                clock::get);
        try {
            windows.add(1);
            windows.add(2);
            assertTrue(firstStarted.await(2, SECONDS));
            windows.add(3);

            clock.set(MAX_WAIT);
            windows.signalCoordinator();
            releaseFirst.countDown();

            assertTrue(secondProcessed.await(2, SECONDS));
            assertEquals(List.of(List.of(1, 2), List.of(3)), processed);
        } finally {
            releaseFirst.countDown();
            windows.close();
        }
    }

    @Test
    void rejectPolicyBoundsInputsUntilProcessingFinishes() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var windows = blockingAccumulator(AdmissionPolicy.REJECT, Duration.ZERO, started, release);
        try {
            windows.add(1);
            assertTrue(started.await(2, SECONDS));

            assertThrows(RejectedExecutionException.class, () -> windows.add(2));
            release.countDown();
            windows.flush();
            windows.add(2);
        } finally {
            release.countDown();
            windows.close();
        }
    }

    @Test
    void waitPolicyResumesAfterProcessingReleasesCapacity() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var windows = blockingAccumulator(AdmissionPolicy.WAIT, Duration.ZERO, started, release);
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            windows.add(1);
            assertTrue(started.await(2, SECONDS));
            var attempted = new CountDownLatch(1);
            var second = callers.submit(() -> {
                attempted.countDown();
                windows.add(2);
                return null;
            });
            assertTrue(attempted.await(2, SECONDS));
            assertThrows(TimeoutException.class, () -> second.get(50, MILLISECONDS));

            release.countDown();
            second.get(2, SECONDS);
        } finally {
            release.countDown();
            windows.close();
        }
    }

    @Test
    void timedWaitExpiresWithoutAdmittingTheInput() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var processed = new AtomicInteger();
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(
                        1,
                        Duration.ZERO,
                        1,
                        AdmissionPolicy.WAIT_WITH_TIMEOUT,
                        Duration.ofMillis(50)),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                sum -> {
                    started.countDown();
                    release.await();
                    processed.addAndGet(sum[0]);
                });
        try {
            windows.add(1);
            assertTrue(started.await(2, SECONDS));

            assertThrows(TimeoutException.class, () -> windows.add(2));
            release.countDown();
            windows.flush();
            assertEquals(1, processed.get());
        } finally {
            release.countDown();
            windows.close();
        }
    }

    @Test
    void interruptedWaitDoesNotAdmitTheInput() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var processed = new AtomicInteger();
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(1, Duration.ZERO, 1, AdmissionPolicy.WAIT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                sum -> {
                    started.countDown();
                    release.await();
                    processed.addAndGet(sum[0]);
                });
        try {
            windows.add(1);
            assertTrue(started.await(2, SECONDS));
            var outcome = new CompletableFuture<Throwable>();
            Thread waiter = Thread.ofVirtual().start(() -> {
                try {
                    windows.add(2);
                    outcome.complete(new AssertionError("interrupted input was admitted"));
                } catch (Throwable failure) {
                    outcome.complete(failure);
                }
            });

            waiter.interrupt();
            assertInstanceOf(InterruptedException.class, outcome.get(2, SECONDS));
            waiter.join();
            release.countDown();
            windows.flush();
            assertEquals(1, processed.get());
        } finally {
            release.countDown();
            windows.close();
        }
    }

    @Test
    void flushHasAnExactBoundaryAndWaitsForPriorProcessing() throws Exception {
        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var processed = new LinkedBlockingQueue<List<Integer>>();
        var windows = new WindowedAccumulator<Integer, List<Integer>>(
                config(3, Duration.ofHours(1), 3, AdmissionPolicy.REJECT, Duration.ZERO),
                ArrayList::new,
                List::add,
                window -> {
                    if (calls.getAndIncrement() == 0) {
                        firstStarted.countDown();
                        releaseFirst.await();
                    }
                    processed.add(List.copyOf(window));
                });
        try (var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            windows.add(1);
            var flush = callers.submit(windows::flush);
            assertTrue(firstStarted.await(2, SECONDS));

            windows.add(2);
            assertThrows(TimeoutException.class, () -> flush.get(50, MILLISECONDS));
            releaseFirst.countDown();
            flush.get(2, SECONDS);

            assertEquals(List.of(1), processed.poll(2, SECONDS));
            assertNull(processed.poll(50, MILLISECONDS));
            windows.close();
            assertEquals(List.of(2), processed.poll(2, SECONDS));
        } finally {
            releaseFirst.countDown();
            windows.close();
        }
    }

    @Test
    void emptyFlushAndCloseDoNotCreateAWindow() {
        var factories = new AtomicInteger();
        var processors = new AtomicInteger();
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(2, Duration.ofSeconds(1), 2, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> {
                    factories.incrementAndGet();
                    return new int[1];
                },
                (sum, input) -> sum[0] += input,
                ignored -> processors.incrementAndGet());

        windows.flush();
        windows.close();

        assertEquals(0, factories.get());
        assertEquals(0, processors.get());
    }

    @Test
    void closeDrainsAPartialWindowAndRestoresInterruptedStatus() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var closeReturned = new CountDownLatch(1);
        var interruptedAfterClose = new AtomicInteger();
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(3, Duration.ofHours(1), 3, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                ignored -> {
                    started.countDown();
                    release.await();
                });
        try {
            windows.add(1);
            Thread closer = Thread.ofVirtual().start(() -> {
                Thread.currentThread().interrupt();
                windows.close();
                interruptedAfterClose.set(Thread.currentThread().isInterrupted() ? 1 : 0);
                closeReturned.countDown();
            });
            assertTrue(started.await(2, SECONDS));
            assertThrows(RejectedExecutionException.class, () -> windows.add(2));
            assertFalse(closeReturned.await(50, MILLISECONDS));

            release.countDown();
            assertTrue(closeReturned.await(2, SECONDS));
            closer.join();
            assertEquals(1, interruptedAfterClose.get());
        } finally {
            release.countDown();
            windows.close();
        }
    }

    @Test
    void accumulatorFailureDoesNotAdmitTheContribution() throws Exception {
        var processed = new LinkedBlockingQueue<Integer>();
        try (var windows = new WindowedAccumulator<Integer, int[]>(
                config(3, Duration.ofHours(1), 3, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> {
                    if (input < 0) {
                        throw new IllegalArgumentException("negative");
                    }
                    sum[0] += input;
                },
                sum -> processed.add(sum[0]))) {
            windows.add(1);
            assertThrows(IllegalArgumentException.class, () -> windows.add(-100));
            windows.add(2);
            windows.flush();

            assertEquals(3, processed.poll(2, SECONDS));
        }
    }

    @Test
    void processorFailureStopsAdmissionButDrainsAlreadyAdmittedWindows() throws Exception {
        var firstStarted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var failure = new IllegalStateException("downstream failed");
        var processed = new AtomicInteger();
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(1, Duration.ZERO, 2, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                sum -> {
                    if (sum[0] == 1) {
                        firstStarted.countDown();
                        releaseFirst.await();
                        throw failure;
                    }
                    processed.addAndGet(sum[0]);
                });
        try {
            windows.add(1);
            assertTrue(firstStarted.await(2, SECONDS));
            windows.add(2);
            releaseFirst.countDown();

            var flushFailure = assertThrows(IllegalStateException.class, windows::flush);
            assertSame(failure, flushFailure.getCause());
            var rejection = assertThrows(RejectedExecutionException.class, () -> windows.add(3));
            assertSame(failure, rejection.getCause());
            assertEquals(2, processed.get());
            var closeFailure = assertThrows(IllegalStateException.class, windows::close);
            assertSame(failure, closeFailure.getCause());
        } finally {
            releaseFirst.countDown();
            try {
                windows.close();
            } catch (IllegalStateException expected) {
                assertSame(failure, expected.getCause());
            }
        }
    }

    @Test
    void rejectsInvalidConfigurationAndNullCollaborators() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> config(
                        0, Duration.ZERO, 1, AdmissionPolicy.REJECT, Duration.ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> config(
                        1, Duration.ZERO, 0, AdmissionPolicy.REJECT, Duration.ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> config(
                        1, Duration.ofNanos(-1), 1, AdmissionPolicy.REJECT, Duration.ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> config(
                        1, Duration.ZERO, 1, AdmissionPolicy.WAIT, Duration.ofNanos(-1))),
                () -> assertThrows(IllegalArgumentException.class, () -> config(
                        1,
                        Duration.ZERO,
                        1,
                        AdmissionPolicy.WAIT_WITH_TIMEOUT,
                        Duration.ZERO)),
                () -> assertThrows(ArithmeticException.class, () -> config(
                        1,
                        Duration.ofSeconds(Long.MAX_VALUE),
                        1,
                        AdmissionPolicy.REJECT,
                        Duration.ZERO)),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulator<>(
                        null, () -> new int[1], (state, input) -> {}, state -> {})),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulator<>(
                        config(1, Duration.ZERO, 1, AdmissionPolicy.REJECT, Duration.ZERO),
                        null,
                        (state, input) -> {},
                        state -> {})),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulator<>(
                        config(1, Duration.ZERO, 1, AdmissionPolicy.REJECT, Duration.ZERO),
                        () -> new int[1],
                        null,
                        state -> {})),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulator<>(
                        config(1, Duration.ZERO, 1, AdmissionPolicy.REJECT, Duration.ZERO),
                        () -> new int[1],
                        (state, input) -> {},
                        null)),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulatorConfig(
                        1, null, 1, AdmissionPolicy.REJECT, Duration.ZERO)),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulatorConfig(
                        1, Duration.ZERO, 1, null, Duration.ZERO)),
                () -> assertThrows(NullPointerException.class, () -> new WindowedAccumulatorConfig(
                        1, Duration.ZERO, 1, AdmissionPolicy.REJECT, null)));
    }

    @Test
    void nullFactoryResultAndNullInputAreNotAdmitted() throws Exception {
        var windows = new WindowedAccumulator<Integer, int[]>(
                config(1, Duration.ZERO, 1, AdmissionPolicy.REJECT, Duration.ZERO),
                () -> null,
                (sum, input) -> sum[0] += input,
                ignored -> {});
        try {
            assertThrows(NullPointerException.class, () -> windows.add(null));
            assertThrows(NullPointerException.class, () -> windows.add(1));
            windows.flush();
        } finally {
            windows.close();
        }
    }

    private static WindowedAccumulator<Integer, List<Integer>> listAccumulator(
            int maxInputs,
            AtomicLong clock,
            LinkedBlockingQueue<List<Integer>> processed) {
        return new WindowedAccumulator<>(
                config(
                        maxInputs,
                        Duration.ofHours(1),
                        8,
                        AdmissionPolicy.REJECT,
                        Duration.ZERO),
                ArrayList::new,
                List::add,
                window -> processed.add(List.copyOf(window)),
                clock::get);
    }

    private static WindowedAccumulator<Integer, int[]> blockingAccumulator(
            AdmissionPolicy policy,
            Duration admissionTimeout,
            CountDownLatch started,
            CountDownLatch release) {
        return new WindowedAccumulator<>(
                config(1, Duration.ZERO, 1, policy, admissionTimeout),
                () -> new int[1],
                (sum, input) -> sum[0] += input,
                ignored -> {
                    started.countDown();
                    release.await();
                });
    }

    private static WindowedAccumulatorConfig config(
            int maxInputs,
            Duration maxWait,
            int maxPendingInputs,
            AdmissionPolicy policy,
            Duration admissionTimeout) {
        return new WindowedAccumulatorConfig(
                maxInputs, maxWait, maxPendingInputs, policy, admissionTimeout);
    }
}
