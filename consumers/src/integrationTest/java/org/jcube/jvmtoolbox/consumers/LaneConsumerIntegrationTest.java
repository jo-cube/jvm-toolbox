package org.jcube.jvmtoolbox.consumers;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Pattern;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LaneConsumerIntegrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final String BOOTSTRAP_SERVERS = bootstrapServers();

    private final List<String> topics = new ArrayList<>();

    @AfterEach
    void deleteTopics() throws Exception {
        if (topics.isEmpty()) {
            return;
        }
        try (var admin = admin()) {
            admin.deleteTopics(topics).all().get(TIMEOUT.toMillis(), MILLISECONDS);
        }
    }

    @Test
    void processesOrderedLanesInParallelAndRestartsFromCommittedOffsets() throws Exception {
        String topic = createTopic(2);
        String group = unique("committed");
        produce(List.of(
                new ProducerRecord<>(topic, 0, 0, "a"),
                new ProducerRecord<>(topic, 1, 1, "c"),
                new ProducerRecord<>(topic, 0, 0, "b"),
                new ProducerRecord<>(topic, 1, 1, "d")));

        var active = new AtomicInteger();
        var maximumActive = new AtomicInteger();
        var activeByLane = new AtomicIntegerArray(2);
        var firstTwoStarted = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        var processed = new CountDownLatch(4);
        var valuesByLane = List.of(
                Collections.synchronizedList(new ArrayList<String>()),
                Collections.synchronizedList(new ArrayList<String>()));
        var failure = new AtomicReference<Throwable>();
        var setup = consumer(topic, group, "ordered", config(2, 1, 2, 4), records -> {
            int lane = Math.floorMod(records.getFirst().key(), 2);
            int laneActive = activeByLane.incrementAndGet(lane);
            try {
                if (laneActive != 1) {
                    throw new AssertionError("lane executions overlapped");
                }
                int current = active.incrementAndGet();
                maximumActive.accumulateAndGet(current, Math::max);
                try {
                    firstTwoStarted.countDown();
                    release.await();
                    valuesByLane.get(lane).add(records.getFirst().value());
                    processed.countDown();
                } finally {
                    active.decrementAndGet();
                }
            } finally {
                activeByLane.decrementAndGet(lane);
            }
        });
        Thread runner = start(setup, failure);
        try {
            await(firstTwoStarted, "two lanes to start");
            assertEquals(2, maximumActive.get());
            release.countDown();
            await(processed, "all records to be processed");
            awaitCommitted(
                    group,
                    Map.of(
                            new TopicPartition(topic, 0), 2L,
                            new TopicPartition(topic, 1), 2L));
        } finally {
            release.countDown();
            setup.close();
            runner.join();
        }
        assertNull(failure.get());
        assertEquals(List.of("a", "b"), valuesByLane.get(0));
        assertEquals(List.of("c", "d"), valuesByLane.get(1));

        produce(List.of(new ProducerRecord<>(topic, 0, 0, "e")));
        var restartedValues = new CopyOnWriteArrayList<String>();
        var restarted = new CountDownLatch(1);
        var restartFailure = new AtomicReference<Throwable>();
        var restart = consumer(topic, group, "restart", config(2, 1, 2, 4), records -> {
            records.forEach(record -> {
                restartedValues.add(record.value());
                restarted.countDown();
            });
        });
        Thread restartRunner = start(restart, restartFailure);
        try {
            await(restarted, "the post-restart record");
        } finally {
            restart.close();
            restartRunner.join();
        }
        assertNull(restartFailure.get());
        assertEquals(List.of("e"), restartedValues);
    }

    @Test
    void shutdownDoesNotCommitActiveWorkAndRestartReplaysIt() throws Exception {
        String topic = createTopic(1);
        String group = unique("shutdown");
        produce(List.of(new ProducerRecord<>(topic, 0, 7, "replay")));

        var started = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        var setup = consumer(topic, group, "shutdown", config(1, 1, 1, 1), records -> {
            started.countDown();
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException expected) {
                interrupted.countDown();
            }
        });
        Thread runner = start(setup, failure);
        try {
            await(started, "active work to start");
            setup.close();
            await(interrupted, "active work to be interrupted");
        } finally {
            setup.close();
            runner.join();
        }
        assertNull(failure.get());

        var replayedValue = new AtomicReference<String>();
        var replayed = new CountDownLatch(1);
        var restartFailure = new AtomicReference<Throwable>();
        var restart = consumer(topic, group, "shutdown-restart", config(1, 1, 1, 1), records -> {
            replayedValue.set(records.getFirst().value());
            replayed.countDown();
        });
        Thread restartRunner = start(restart, restartFailure);
        try {
            await(replayed, "uncommitted work to replay");
        } finally {
            restart.close();
            restartRunner.join();
        }
        assertNull(restartFailure.get());
        assertEquals("replay", replayedValue.get());
    }

    @Test
    void brokerRebalanceInvalidatesAndReplaysAnActiveMixedBatch() throws Exception {
        String topic = createTopic(2);
        String group = unique("rebalance");
        produce(List.of(
                new ProducerRecord<>(topic, 0, 0, "zero"),
                new ProducerRecord<>(topic, 1, 0, "one")));

        var invocations = new AtomicInteger();
        var firstBatchPartitions = new AtomicReference<Set<Integer>>();
        var firstBatchStarted = new CountDownLatch(1);
        var firstBatchInterrupted = new CountDownLatch(1);
        var replayed = new CountDownLatch(2);
        var successfulValues = new CopyOnWriteArrayList<String>();
        ConsumerBatchProcessor<Integer, String> processor = records -> {
            if (invocations.getAndIncrement() == 0) {
                var partitions = new HashSet<Integer>();
                records.forEach(record -> partitions.add(record.partition()));
                firstBatchPartitions.set(Set.copyOf(partitions));
                firstBatchStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException expected) {
                    firstBatchInterrupted.countDown();
                    return;
                }
                throw new AssertionError("active batch was not interrupted by the rebalance");
            }
            records.forEach(record -> {
                successfulValues.add(record.value());
                replayed.countDown();
            });
        };

        var firstFailure = new AtomicReference<Throwable>();
        var secondFailure = new AtomicReference<Throwable>();
        var first = consumer(
                topic,
                group,
                "rebalance-a",
                new ConsumerProcessingConfig(
                        1,
                        2,
                        Duration.ofSeconds(5),
                        1,
                        2,
                        2,
                        2,
                        Duration.ofMillis(100)),
                processor);
        var second = consumer(topic, group, "rebalance-b", config(1, 1, 1, 2), processor);
        Thread firstRunner = start(first, firstFailure);
        Thread secondRunner = null;
        try {
            await(firstBatchStarted, "the mixed batch to start");
            assertEquals(Set.of(0, 1), firstBatchPartitions.get());
            secondRunner = start(second, secondFailure);
            await(firstBatchInterrupted, "the broker rebalance to invalidate the active batch");
            await(replayed, "both invalidated records to replay");
            awaitCommitted(
                    group,
                    Map.of(
                            new TopicPartition(topic, 0), 1L,
                            new TopicPartition(topic, 1), 1L));
        } finally {
            second.close();
            first.close();
            if (secondRunner != null) {
                secondRunner.join();
            }
            firstRunner.join();
        }
        assertNull(firstFailure.get());
        assertNull(secondFailure.get());
        assertEquals(2, successfulValues.size());
        assertEquals(Set.of("zero", "one"), Set.copyOf(successfulValues));
    }

    private String createTopic(int partitions) throws Exception {
        String topic = unique("topic");
        topics.add(topic);
        try (var admin = admin()) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all()
                    .get(TIMEOUT.toMillis(), MILLISECONDS);
        }
        return topic;
    }

    private static Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));
    }

    private static void produce(List<ProducerRecord<Integer, String>> records) throws Exception {
        var properties = new HashMap<String, Object>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        try (var producer = new KafkaProducer<>(
                properties, new IntegerSerializer(), new StringSerializer())) {
            for (ProducerRecord<Integer, String> record : records) {
                producer.send(record).get(TIMEOUT.toMillis(), MILLISECONDS);
            }
        }
    }

    private static LaneConsumer<Integer, String> consumer(
            String topic,
            String group,
            String client,
            ConsumerProcessingConfig config,
            ConsumerBatchProcessor<Integer, String> processor) {
        var properties = new HashMap<String, Object>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, client + "-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        properties.put(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                RangeAssignor.class.getName());
        return new LaneConsumer<>(
                properties,
                new IntegerDeserializer(),
                new StringDeserializer(),
                Pattern.compile(Pattern.quote(topic)),
                config,
                LaneRouter.byKeyHashCode(),
                processor);
    }

    private static ConsumerProcessingConfig config(
            int lanes, int batchSize, int concurrentBatches, int maxPollRecords) {
        return new ConsumerProcessingConfig(
                lanes,
                batchSize,
                Duration.ZERO,
                concurrentBatches,
                maxPollRecords,
                maxPollRecords,
                maxPollRecords,
                Duration.ofMillis(100));
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

    private static void await(CountDownLatch latch, String description) throws InterruptedException {
        assertTrue(
                latch.await(TIMEOUT.toMillis(), MILLISECONDS),
                "timed out waiting for " + description);
    }

    private static void awaitCommitted(String group, Map<TopicPartition, Long> expected)
            throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<TopicPartition, OffsetAndMetadata> committed = Map.of();
        try (var admin = admin()) {
            while (System.nanoTime() < deadline) {
                Map<TopicPartition, OffsetAndMetadata> observed = admin.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata()
                        .get(TIMEOUT.toMillis(), MILLISECONDS);
                committed = observed;
                boolean complete = expected.entrySet().stream().allMatch(entry -> {
                    OffsetAndMetadata offset = observed.get(entry.getKey());
                    return offset != null && offset.offset() == entry.getValue();
                });
                if (complete) {
                    return;
                }
                LockSupport.parkNanos(Duration.ofMillis(25).toNanos());
            }
        }
        fail("timed out waiting for committed offsets " + expected + "; observed " + committed);
    }

    private static String unique(String purpose) {
        return "jvm-toolbox-" + purpose + "-" + UUID.randomUUID();
    }

    private static String bootstrapServers() {
        String configured = System.getenv("JVM_TOOLBOX_KAFKA_BOOTSTRAP_SERVERS");
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        return "localhost:"
                + System.getenv().getOrDefault("JVM_TOOLBOX_KAFKA_PORT", "59092");
    }
}
