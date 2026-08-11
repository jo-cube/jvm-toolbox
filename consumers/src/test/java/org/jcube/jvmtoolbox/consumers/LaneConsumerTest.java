package org.jcube.jvmtoolbox.consumers;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
class LaneConsumerTest {
    private static final String TOPIC = "events";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);

    @Test
    void closeBeforeRunClosesOwnedDeserializers() {
        var keyDeserializer = new ClosingDeserializer<Integer>();
        var valueDeserializer = new ClosingDeserializer<String>();
        var setup = new LaneConsumer<>(
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                keyDeserializer,
                valueDeserializer,
                List.of(TOPIC),
                config(1, 1, 1, 1, 1, 1),
                LaneRouter.byKeyHashCode(),
                ignored -> {});

        setup.close();
        setup.close();

        assertEquals(1, keyDeserializer.closeCalls);
        assertEquals(1, valueDeserializer.closeCalls);
    }

    @Test
    void consumerConstructionFailureClosesOwnedDeserializers() {
        var expected = new IllegalStateException("construction failed");
        var keyDeserializer = new ClosingDeserializer<Integer>();
        var valueDeserializer = new ClosingDeserializer<String>();
        var setup = new LaneConsumer<>(
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                keyDeserializer,
                valueDeserializer,
                List.of(TOPIC),
                config(1, 1, 1, 1, 1, 1),
                LaneRouter.byKeyHashCode(),
                ignored -> {},
                (properties, key, value) -> {
                    throw expected;
                },
                System::nanoTime);

        assertSame(expected, assertThrows(IllegalStateException.class, setup::run));
        assertEquals(1, keyDeserializer.closeCalls);
        assertEquals(1, valueDeserializer.closeCalls);
    }

    @Test
    void validatesManagedKafkaPropertiesWithoutLossyCoercion() {
        var config = config(1, 1, 1, 1, 1, 1);
        var setup = new LaneConsumer<>(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, " false ",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, " 1 "),
                new IntegerDeserializer(),
                new StringDeserializer(),
                List.of(TOPIC),
                config,
                LaneRouter.byKeyHashCode(),
                ignored -> {});
        setup.close();

        assertThrows(IllegalArgumentException.class, () -> new LaneConsumer<>(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 4_294_967_297L),
                new IntegerDeserializer(),
                new StringDeserializer(),
                List.of(TOPIC),
                config,
                LaneRouter.byKeyHashCode(),
                ignored -> {}));
    }

    @Test
    void preservesLaneOrderAndBoundsCrossLaneConcurrency() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var active = new AtomicInteger();
        var maximum = new AtomicInteger();
        var activeByLane = new AtomicIntegerArray(2);
        var firstTwoStarted = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var processed = new CountDownLatch(4);
        var valuesByLane = List.of(
                Collections.synchronizedList(new ArrayList<String>()),
                Collections.synchronizedList(new ArrayList<String>()));
        var config = config(2, 1, 2, 8, 8, 4);
        var setup = setup(mock, config, record -> record.key(), records -> {
            int lane = Math.floorMod(records.getFirst().key(), 2);
            if (activeByLane.incrementAndGet(lane) != 1) {
                throw new AssertionError("lane executions overlapped");
            }
            int current = active.incrementAndGet();
            maximum.accumulateAndGet(current, Math::max);
            firstTwoStarted.countDown();
            release.await();
            valuesByLane.get(lane).add(records.getFirst().value());
            active.decrementAndGet();
            activeByLane.decrementAndGet(lane);
            processed.countDown();
        });
        mock.initial(
                List.of(P0),
                List.of(
                        record(0, 0, 0, "a"),
                        record(0, 1, 1, "c"),
                        record(0, 2, 0, "b"),
                        record(0, 3, 1, "d")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);
        try {
            assertTrue(firstTwoStarted.await(2, SECONDS));
            assertEquals(2, maximum.get());
            release.countDown();
            assertTrue(processed.await(2, SECONDS));
            assertEquals(4, mock.awaitCommit(P0, 4));
            assertEquals(List.of("a", "b"), valuesByLane.get(0));
            assertEquals(List.of("c", "d"), valuesByLane.get(1));
        } finally {
            release.countDown();
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
    }

    @Test
    void commitsOnlyToTheFirstIncompleteSparseOffset() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var laterCompleted = new CountDownLatch(4);
        var blockedStarted = new CountDownLatch(1);
        var releaseBlocked = new CountDownLatch(1);
        var config = config(8, 1, 5, 10, 10, 5);
        var setup = setup(mock, config, record -> record.key(), records -> {
            long offset = records.getFirst().offset();
            if (offset == 109) {
                blockedStarted.countDown();
                releaseBlocked.await();
            } else {
                laterCompleted.countDown();
            }
        });
        mock.initial(
                List.of(P0),
                List.of(
                        record(0, 100, 0, "100"),
                        record(0, 105, 1, "105"),
                        record(0, 109, 2, "109"),
                        record(0, 120, 3, "120"),
                        record(0, 135, 4, "135")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);
        try {
            assertTrue(blockedStarted.await(2, SECONDS));
            assertTrue(laterCompleted.await(2, SECONDS));
            assertEquals(109, mock.awaitCommit(P0, 109));
            assertTrue(mock.commits.stream()
                    .flatMap(commit -> commit.values().stream())
                    .allMatch(offset -> offset.offset() <= 109));

            releaseBlocked.countDown();
            assertEquals(136, mock.awaitCommit(P0, 136));
        } finally {
            releaseBlocked.countDown();
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
    }

    @Test
    void revocationInvalidatesTheWholeMixedBatchAndReplaysStillOwnedRecords() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var firstBatchStarted = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var replayed = new CountDownLatch(1);
        var invocations = new AtomicInteger();
        var config = config(1, 2, 1, 4, 4, 2);
        var setup = setup(mock, config, ignored -> 0, records -> {
            if (invocations.incrementAndGet() == 1) {
                assertEquals(List.of(0, 1), records.stream().map(ConsumerRecord::partition).toList());
                firstBatchStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException expected) {
                    interrupted.countDown();
                }
            } else {
                assertEquals(List.of(P1), records.stream()
                        .map(record -> new TopicPartition(record.topic(), record.partition()))
                        .toList());
                replayed.countDown();
            }
        });
        mock.initial(
                List.of(P0, P1),
                List.of(record(0, 0, 1, "revoked"), record(1, 0, 1, "retained")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);
        try {
            assertTrue(firstBatchStarted.await(2, SECONDS));
            mock.schedulePollTask(() -> mock.revokeOnly(List.of(P0)));
            assertTrue(interrupted.await(2, SECONDS));
            assertTrue(replayed.await(2, SECONDS));
            assertEquals(1, mock.awaitCommit(P1, 1));
            assertTrue(mock.commits.stream().noneMatch(commit -> commit.containsKey(P0)));
        } finally {
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
    }

    @Test
    void pausesUntilAFullPollFitsAgain() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var firstStarted = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var processed = new CountDownLatch(2);
        var config = config(1, 1, 1, 2, 2, 2);
        var setup = setup(mock, config, ignored -> 0, records -> {
            if (records.getFirst().offset() == 0) {
                firstStarted.countDown();
                release.await();
            }
            processed.countDown();
        });
        mock.initial(List.of(P0), List.of(record(0, 0, 1, "a"), record(0, 1, 1, "b")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);
        try {
            assertTrue(firstStarted.await(2, SECONDS));
            assertTrue(mock.pauses.poll(2, SECONDS).contains(P0));
            release.countDown();
            assertTrue(processed.await(2, SECONDS));
            assertTrue(mock.resumes.poll(2, SECONDS).contains(P0));
        } finally {
            release.countDown();
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
    }

    @Test
    void processorFailureFailsTheSetupWithoutCommitting() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var expected = new IOException("backend failed");
        var setup = setup(mock, config(1, 1, 1, 2, 2, 1), ignored -> 0, records -> {
            throw expected;
        });
        mock.initial(List.of(P0), List.of(record(0, 0, 1, "failed")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);

        runner.join(Duration.ofSeconds(2));

        assertFalse(runner.isAlive());
        assertSame(expected, failure.get());
        assertTrue(mock.commits.isEmpty());
        assertTrue(mock.closed());
    }

    @Test
    void closeInterruptsWithoutDrainingOrFinalCommit() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var started = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var setup = setup(mock, config(1, 1, 1, 2, 2, 1), ignored -> 0, records -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                interrupted.countDown();
            }
        });
        mock.initial(List.of(P0), List.of(record(0, 0, 1, "unfinished")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);

        assertTrue(started.await(2, SECONDS));
        setup.close();
        runner.join();

        assertTrue(interrupted.await(2, SECONDS));
        assertTrue(mock.commits.isEmpty());
        assertNull(failure.get());
    }

    @Test
    void dispatchesAPartialBatchWhenItsOldestRecordReachesTheWait() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var now = new AtomicLong();
        var processed = new CountDownLatch(1);
        var config = new ConsumerProcessingConfig(
                1, 10, Duration.ofNanos(10), 1, 10, 10, 10, Duration.ofMillis(10));
        var setup = setup(mock, config, ignored -> 0, records -> {
            assertEquals(List.of("ready"), records.stream().map(ConsumerRecord::value).toList());
            processed.countDown();
        }, now::get);
        mock.initial(List.of(P0), List.of(record(0, 0, 1, "ready")));
        mock.schedulePollTask(() -> now.set(10));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);
        try {
            assertTrue(processed.await(2, SECONDS));
        } finally {
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
    }

    @Test
    void fencesACompletionFromAnOlderAssignmentOfTheSamePartition() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var firstStarted = new CountDownLatch(1);
        var firstInterrupted = new CountDownLatch(1);
        var releaseFirst = new CountDownLatch(1);
        var replayed = new CountDownLatch(1);
        var invocations = new AtomicInteger();
        var setup = setup(mock, config(1, 1, 1, 2, 2, 1), ignored -> 0, records -> {
            if (invocations.incrementAndGet() == 1) {
                firstStarted.countDown();
                while (true) {
                    try {
                        releaseFirst.await();
                        break;
                    } catch (InterruptedException ignored) {
                        firstInterrupted.countDown();
                    }
                }
            } else {
                assertEquals("new assignment", records.getFirst().value());
                replayed.countDown();
            }
        });
        mock.initial(List.of(P0), List.of(record(0, 0, 1, "old assignment")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);
        try {
            assertTrue(firstStarted.await(2, SECONDS));
            mock.schedulePollTask(() -> mock.reassign(
                    P0, record(0, 0, 1, "new assignment")));
            assertTrue(firstInterrupted.await(2, SECONDS));
            releaseFirst.countDown();
            assertTrue(replayed.await(2, SECONDS));
            assertEquals(1, mock.awaitCommit(P0, 1));
            assertEquals(2, invocations.get());
        } finally {
            releaseFirst.countDown();
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
    }

    @Test
    void commitFailureFailsTheSetup() throws Exception {
        var mock = new TestConsumer<Integer, String>();
        var expected = new IllegalStateException("commit failed");
        mock.commitFailure = expected;
        var setup = setup(mock, config(1, 1, 1, 2, 2, 1), ignored -> 0, ignored -> {});
        mock.initial(List.of(P0), List.of(record(0, 0, 1, "processed")));
        var failure = new AtomicReference<Throwable>();
        Thread runner = start(setup, failure);

        runner.join(Duration.ofSeconds(2));

        assertFalse(runner.isAlive());
        assertSame(expected, failure.get());
        assertTrue(mock.closed());
    }

    private static LaneConsumer<Integer, String> setup(
            TestConsumer<Integer, String> mock,
            ConsumerProcessingConfig config,
            LaneRouter<Integer, String> router,
            ConsumerBatchProcessor<Integer, String> processor) {
        return setup(mock, config, router, processor, System::nanoTime);
    }

    private static LaneConsumer<Integer, String> setup(
            TestConsumer<Integer, String> mock,
            ConsumerProcessingConfig config,
            LaneRouter<Integer, String> router,
            ConsumerBatchProcessor<Integer, String> processor,
            java.util.function.LongSupplier nanoClock) {
        mock.setMaxPollRecords(config.maxPollRecords());
        return new LaneConsumer<>(
                Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                new IntegerDeserializer(),
                new StringDeserializer(),
                List.of(TOPIC),
                config,
                router,
                processor,
                (properties, keyDeserializer, valueDeserializer) -> mock,
                nanoClock);
    }

    private static ConsumerProcessingConfig config(
            int laneCount,
            int maxBatchSize,
            int maxConcurrentBatches,
            int maxInFlight,
            int maxInFlightPerPartition,
            int maxPollRecords) {
        return new ConsumerProcessingConfig(
                laneCount,
                maxBatchSize,
                Duration.ZERO,
                maxConcurrentBatches,
                maxInFlight,
                maxInFlightPerPartition,
                maxPollRecords,
                Duration.ofMillis(10));
    }

    private static ConsumerRecord<Integer, String> record(
            int partition, long offset, int key, String value) {
        return new ConsumerRecord<>(TOPIC, partition, offset, key, value);
    }

    private static Thread start(
            LaneConsumer<Integer, String> setup, AtomicReference<Throwable> failure) {
        return Thread.ofVirtual().start(() -> {
            try {
                setup.run();
            } catch (Throwable caught) {
                failure.set(caught);
            }
        });
    }

    private static final class TestConsumer<K, V> extends MockConsumer<K, V> {
        private final List<Map<TopicPartition, OffsetAndMetadata>> commits =
                new CopyOnWriteArrayList<>();
        private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> commitEvents =
                new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Set<TopicPartition>> pauses = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Set<TopicPartition>> resumes = new LinkedBlockingQueue<>();
        private volatile ConsumerRebalanceListener listener;
        private volatile RuntimeException commitFailure;

        private TestConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public void subscribe(
                Collection<String> topics, ConsumerRebalanceListener listener) {
            this.listener = listener;
            super.subscribe(topics, listener);
        }

        @Override
        public synchronized void commitSync(
                Map<TopicPartition, OffsetAndMetadata> offsets) {
            if (commitFailure != null) {
                RuntimeException failure = commitFailure;
                commitFailure = null;
                throw failure;
            }
            Map<TopicPartition, OffsetAndMetadata> copy = Map.copyOf(offsets);
            commits.add(copy);
            commitEvents.add(copy);
            super.commitSync(offsets);
        }

        @Override
        public synchronized void pause(Collection<TopicPartition> partitions) {
            pauses.add(Set.copyOf(partitions));
            super.pause(partitions);
        }

        @Override
        public synchronized void resume(Collection<TopicPartition> partitions) {
            resumes.add(Set.copyOf(partitions));
            super.resume(partitions);
        }

        private void initial(
                List<TopicPartition> partitions, List<ConsumerRecord<K, V>> records) {
            var beginnings = new java.util.HashMap<TopicPartition, Long>();
            for (TopicPartition partition : partitions) {
                long firstOffset = records.stream()
                        .filter(record -> record.topic().equals(partition.topic())
                                && record.partition() == partition.partition())
                        .mapToLong(ConsumerRecord::offset)
                        .min()
                        .orElse(0);
                beginnings.put(partition, firstOffset);
            }
            updateBeginningOffsets(beginnings);
            schedulePollTask(() -> {
                rebalance(partitions);
                records.forEach(this::addRecord);
            });
        }

        private void revokeOnly(Collection<TopicPartition> partitions) {
            listener.onPartitionsRevoked(partitions);
        }

        private void reassign(TopicPartition partition, ConsumerRecord<K, V> record) {
            listener.onPartitionsRevoked(List.of(partition));
            listener.onPartitionsAssigned(List.of(partition));
            seek(partition, record.offset());
            addRecord(record);
        }

        private long awaitCommit(TopicPartition partition, long expected) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (true) {
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "timed out waiting for commit " + expected);
                Map<TopicPartition, OffsetAndMetadata> commit =
                        commitEvents.poll(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
                assertTrue(commit != null, "timed out waiting for commit " + expected);
                OffsetAndMetadata offset = commit.get(partition);
                if (offset != null && offset.offset() == expected) {
                    return offset.offset();
                }
            }
        }
    }

    private static final class ClosingDeserializer<T> implements Deserializer<T> {
        private int closeCalls;

        @Override
        public T deserialize(String topic, byte[] data) {
            return null;
        }

        @Override
        public void close() {
            closeCalls++;
        }
    }
}
