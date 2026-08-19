package org.jcube.jvmtoolbox.consumers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Consumes Kafka records into deterministic ordered lanes and processes micro-batches in parallel.
 *
 * <p>{@link #run()} is a blocking call and exclusively owns consumer subscription, polling,
 * assignment, flow control, and commits. A record's non-null key is routed to one logical lane.
 * Processor invocations within a lane never overlap; different lanes may execute concurrently on
 * virtual threads.
 *
 * <p>Offsets are committed only to the first incomplete observed record in each partition, or to the
 * poll's next position when all observed records complete. Partial-frontier commits carry the last
 * completed record's leader epoch when available. Automatic commits are disabled. Batch, routing,
 * polling, tracking, and commit failures fail {@code run()}.
 *
 * <p>Partition revocation invalidates any active mixed-partition batch containing a revoked record and
 * interrupts its virtual thread. Stale completions cannot update offsets. Interruption is cooperative,
 * so downstream effects must tolerate replay and possible overlap during reassignment.
 *
 * <p>{@link #close()} stops polling, interrupts active batches, closes the owned Kafka consumer, and
 * waits for the owner loop to terminate. It neither drains work nor performs a final commit. The
 * setup owns the supplied deserializers but not resources captured by the processor.
 *
 * @param <K> Kafka key type
 * @param <V> Kafka value type
 */
public final class LaneConsumer<K, V> implements AutoCloseable {
    private enum Lifecycle {
        NEW,
        RUNNING,
        STOPPING,
        TERMINATED
    }

    @FunctionalInterface
    interface ConsumerFactory<K, V> {
        Consumer<K, V> create(
                Map<String, Object> properties,
                Deserializer<K> keyDeserializer,
                Deserializer<V> valueDeserializer);
    }

    private final ConsumerProcessingConfig config;
    private final Map<String, Object> properties;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final BiConsumer<Consumer<K, V>, ConsumerRebalanceListener> subscription;
    private final LaneRouter<K, V> router;
    private final ConsumerBatchProcessor<K, V> processor;
    private final ConsumerFactory<K, V> consumerFactory;
    private final LongSupplier nanoClock;
    private final long maxBatchWaitNanos;
    private final long pollTimeoutNanos;
    private final List<Lane<K, V>> lanes;
    private final Map<TopicPartition, PartitionState> partitions = new HashMap<>();
    private final Set<TopicPartition> paused = new HashSet<>();
    private final ConcurrentLinkedQueue<Completion<K, V>> completions =
            new ConcurrentLinkedQueue<>();
    private final AtomicReference<Throwable> asynchronousFailure = new AtomicReference<>();
    private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>(Lifecycle.NEW);
    private final AtomicReference<Consumer<K, V>> activeConsumer = new AtomicReference<>();
    private final CountDownLatch terminated = new CountDownLatch(1);

    private volatile Thread ownerThread;
    private long assignmentEpoch;
    private int activeBatches;
    private int globalInFlight;
    private int nextLane;

    /**
     * Creates a consumer setup. The Kafka consumer is constructed when {@link #run()} begins.
     *
     * @param properties Kafka consumer properties; {@code group.id} must be present, automatic commit
     *     must be absent or false, and {@code max.poll.records}, if present, must match {@code config}
     * @param keyDeserializer owned key deserializer
     * @param valueDeserializer owned value deserializer
     * @param topics non-empty topic subscription
     * @param config processing and buffering limits
     * @param router deterministic logical-lane router
     * @param processor ordered batch operation
     * @throws IllegalArgumentException for invalid or conflicting consumer properties or topics
     * @throws NullPointerException if any argument or collection element is {@code null}
     */
    public LaneConsumer(
            Map<String, ?> properties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            Collection<String> topics,
            ConsumerProcessingConfig config,
            LaneRouter<K, V> router,
            ConsumerBatchProcessor<K, V> processor) {
        this(
                properties,
                keyDeserializer,
                valueDeserializer,
                topicSubscription(topics),
                config,
                router,
                processor,
                KafkaConsumer::new,
                System::nanoTime);
    }

    /**
     * Creates a consumer setup using Kafka's regular-expression topic subscription. The Kafka
     * consumer is constructed when {@link #run()} begins.
     *
     * @param properties Kafka consumer properties; {@code group.id} must be present, automatic commit
     *     must be absent or false, and {@code max.poll.records}, if present, must match {@code config}
     * @param keyDeserializer owned key deserializer
     * @param valueDeserializer owned value deserializer
     * @param topicPattern topic-name pattern passed to Kafka
     * @param config processing and buffering limits
     * @param router deterministic logical-lane router
     * @param processor ordered batch operation
     * @throws IllegalArgumentException for invalid or conflicting consumer properties
     * @throws NullPointerException if any argument is {@code null}
     */
    public LaneConsumer(
            Map<String, ?> properties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            Pattern topicPattern,
            ConsumerProcessingConfig config,
            LaneRouter<K, V> router,
            ConsumerBatchProcessor<K, V> processor) {
        this(
                properties,
                keyDeserializer,
                valueDeserializer,
                patternSubscription(topicPattern),
                config,
                router,
                processor,
                KafkaConsumer::new,
                System::nanoTime);
    }

    LaneConsumer(
            Map<String, ?> properties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            Collection<String> topics,
            ConsumerProcessingConfig config,
            LaneRouter<K, V> router,
            ConsumerBatchProcessor<K, V> processor,
            ConsumerFactory<K, V> consumerFactory,
            LongSupplier nanoClock) {
        this(
                properties,
                keyDeserializer,
                valueDeserializer,
                topicSubscription(topics),
                config,
                router,
                processor,
                consumerFactory,
                nanoClock);
    }

    private LaneConsumer(
            Map<String, ?> properties,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer,
            BiConsumer<Consumer<K, V>, ConsumerRebalanceListener> subscription,
            ConsumerProcessingConfig config,
            LaneRouter<K, V> router,
            ConsumerBatchProcessor<K, V> processor,
            ConsumerFactory<K, V> consumerFactory,
            LongSupplier nanoClock) {
        this.config = Objects.requireNonNull(config, "config");
        this.properties = validatedProperties(properties, config);
        this.keyDeserializer = Objects.requireNonNull(keyDeserializer, "keyDeserializer");
        this.valueDeserializer = Objects.requireNonNull(valueDeserializer, "valueDeserializer");
        this.subscription = Objects.requireNonNull(subscription, "subscription");
        this.router = Objects.requireNonNull(router, "router");
        this.processor = Objects.requireNonNull(processor, "processor");
        this.consumerFactory = Objects.requireNonNull(consumerFactory, "consumerFactory");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        maxBatchWaitNanos = config.maxBatchWait().toNanos();
        pollTimeoutNanos = config.pollTimeout().toNanos();
        var laneList = new ArrayList<Lane<K, V>>(config.laneCount());
        for (int index = 0; index < config.laneCount(); index++) {
            laneList.add(new Lane<>(index));
        }
        lanes = List.copyOf(laneList);
    }

    /**
     * Runs the consumer loop on the calling thread until closed or failed.
     *
     * <p>This method may be invoked once. A normal return means {@link #close()} requested shutdown.
     * Processor and Kafka failures are rethrown after the consumer is closed and active batches have
     * been interrupted; active processor calls are not joined.
     *
     * @throws Exception for a checked processor failure
     * @throws RuntimeException for Kafka, routing, tracking, or unchecked close failure
     * @throws Error for an unrecoverable processor or runtime error
     * @throws IllegalStateException if this setup was already run or closed
     */
    public void run() throws Exception {
        if (!lifecycle.compareAndSet(Lifecycle.NEW, Lifecycle.RUNNING)) {
            throw new IllegalStateException("consumer setup has already been run or closed");
        }
        ownerThread = Thread.currentThread();
        Consumer<K, V> consumer = null;
        Throwable failure = null;
        try {
            consumer = consumerFactory.create(properties, keyDeserializer, valueDeserializer);
            activeConsumer.set(consumer);
            if (lifecycle.get() == Lifecycle.RUNNING) {
                subscription.accept(consumer, new AssignmentListener(consumer));
                failure = runLoop(consumer);
            }
        } catch (Throwable startFailure) {
            failure = startFailure;
        } finally {
            lifecycle.compareAndSet(Lifecycle.RUNNING, Lifecycle.STOPPING);
            cancelAll();
            try {
                if (consumer == null) {
                    closeDeserializers();
                } else {
                    consumer.close();
                }
            } catch (Throwable closeFailure) {
                failure = merge(failure, closeFailure);
            }
            activeConsumer.set(null);
            ownerThread = null;
            lifecycle.set(Lifecycle.TERMINATED);
            terminated.countDown();
        }
        rethrow(failure);
    }

    /**
     * Stops this setup without draining or a final commit and waits for the owner loop to terminate.
     *
     * <p>This method is idempotent. Closing before {@link #run()} prevents it from starting and closes
     * the supplied deserializers. If interrupted while waiting, this method still waits for consumer
     * closure and restores the caller's interrupted status. Invoking this method on the owner thread
     * requests shutdown without waiting on itself.
     *
     * @throws RuntimeException if a supplied deserializer fails to close before {@code run()}
     */
    @Override
    public void close() {
        while (true) {
            Lifecycle current = lifecycle.get();
            if (current == Lifecycle.NEW) {
                if (lifecycle.compareAndSet(Lifecycle.NEW, Lifecycle.TERMINATED)) {
                    try {
                        closeDeserializers();
                    } finally {
                        terminated.countDown();
                    }
                    return;
                }
            } else if (current == Lifecycle.RUNNING) {
                if (lifecycle.compareAndSet(Lifecycle.RUNNING, Lifecycle.STOPPING)) {
                    wakeOwner();
                    break;
                }
            } else {
                break;
            }
        }
        if (Thread.currentThread() != ownerThread) {
            awaitTermination();
        }
    }

    private Throwable runLoop(Consumer<K, V> consumer) {
        while (lifecycle.get() == Lifecycle.RUNNING) {
            try {
                Throwable signalFailure = asynchronousFailure.getAndSet(null);
                if (signalFailure != null) {
                    return signalFailure;
                }
                Throwable processorFailure = drainCompletions();
                if (processorFailure != null) {
                    return processorFailure;
                }
                dispatchReady();
                commitDirty(consumer);
                applyBackpressure(consumer);
                if (lifecycle.get() != Lifecycle.RUNNING) {
                    break;
                }
                ConsumerRecords<K, V> records = consumer.poll(nextPollTimeout());
                if (lifecycle.get() == Lifecycle.RUNNING) {
                    try {
                        accept(records);
                    } catch (WakeupException routingFailure) {
                        return routingFailure;
                    }
                }
            } catch (WakeupException controlSignal) {
                if (lifecycle.get() != Lifecycle.RUNNING) {
                    break;
                }
            } catch (Throwable failure) {
                return failure;
            }
        }
        return null;
    }

    private void accept(ConsumerRecords<K, V> records) {
        long observedAt = nanoClock.getAsLong();
        int accepted = 0;
        for (ConsumerRecord<K, V> record : records) {
            if (++accepted > config.maxPollRecords()) {
                throw new IllegalStateException("Kafka poll exceeded configured maxPollRecords");
            }
            if (record.key() == null) {
                throw new IllegalStateException("null Kafka keys are not supported");
            }
            var topicPartition = new TopicPartition(record.topic(), record.partition());
            PartitionState partition = partitions.get(topicPartition);
            if (partition == null) {
                throw new IllegalStateException("received a record for an unassigned partition: "
                        + topicPartition);
            }
            if (record.offset() <= partition.lastObservedOffset) {
                throw new IllegalStateException("non-monotonic offset " + record.offset() + " for "
                        + topicPartition);
            }
            int laneIndex = Math.floorMod(router.route(record), lanes.size());
            var offset = new TrackedOffset(record.offset(), record.leaderEpoch());
            partition.offsets.addLast(offset);
            partition.lastObservedOffset = record.offset();
            globalInFlight++;
            lanes.get(laneIndex)
                    .queue
                    .addLast(new Envelope<>(record, observedAt, partition, offset));
        }
        for (var nextOffset : records.nextOffsets().entrySet()) {
            PartitionState partition = partitions.get(nextOffset.getKey());
            if (partition != null) {
                partition.observedPosition = nextOffset.getValue();
            }
        }
    }

    private void dispatchReady() {
        while (activeBatches < config.maxConcurrentBatches()) {
            Lane<K, V> lane = nextReadyLane(nanoClock.getAsLong());
            if (lane == null) {
                return;
            }
            startBatch(lane);
        }
    }

    private Lane<K, V> nextReadyLane(long now) {
        for (int scanned = 0; scanned < lanes.size(); scanned++) {
            int index = Math.floorMod(nextLane++, lanes.size());
            Lane<K, V> lane = lanes.get(index);
            if (lane.active == null
                    && !lane.queue.isEmpty()
                    && (lane.queue.size() >= config.maxBatchSize()
                            || remainingWait(lane.queue.getFirst().observedAt, now) == 0)) {
                return lane;
            }
        }
        return null;
    }

    private void startBatch(Lane<K, V> lane) {
        int size = Math.min(config.maxBatchSize(), lane.queue.size());
        var envelopes = new ArrayList<Envelope<K, V>>(size);
        var records = new ArrayList<ConsumerRecord<K, V>>(size);
        for (int index = 0; index < size; index++) {
            Envelope<K, V> envelope = lane.queue.removeFirst();
            envelopes.add(envelope);
            records.add(envelope.record);
        }
        var batch = new Batch<>(lane, List.copyOf(envelopes));
        Thread worker = Thread.ofVirtual()
                .name("jvm-toolbox-consumer-lane-" + lane.index)
                .unstarted(() -> process(batch, List.copyOf(records)));
        batch.thread = worker;
        lane.active = batch;
        activeBatches++;
        try {
            worker.start();
        } catch (Throwable failure) {
            lane.active = null;
            activeBatches--;
            throw failure;
        }
    }

    private void process(Batch<K, V> batch, List<ConsumerRecord<K, V>> records) {
        Throwable failure = null;
        try {
            processor.process(records);
        } catch (Throwable processorFailure) {
            failure = processorFailure;
        }
        var completion = new Completion<>(batch, failure);
        completions.add(completion);
        if (lifecycle.get() == Lifecycle.RUNNING) {
            wakeOwner();
        } else {
            completions.clear();
        }
    }

    private Throwable drainCompletions() {
        Completion<K, V> completion;
        while ((completion = completions.poll()) != null) {
            Batch<K, V> batch = completion.batch;
            Lane<K, V> lane = batch.lane;
            if (lane.active != batch) {
                continue;
            }
            lane.active = null;
            activeBatches--;
            if (batch.invalidated || stale(batch)) {
                requeueStillOwned(batch);
            } else if (completion.failure != null) {
                return completion.failure;
            } else {
                complete(batch);
            }
        }
        return null;
    }

    private boolean stale(Batch<K, V> batch) {
        for (Envelope<K, V> envelope : batch.envelopes) {
            PartitionState current = partitions.get(envelope.partition.topicPartition);
            if (current == null || current.assignmentEpoch != envelope.partition.assignmentEpoch) {
                return true;
            }
        }
        return false;
    }

    private void requeueStillOwned(Batch<K, V> batch) {
        List<Envelope<K, V>> envelopes = batch.envelopes;
        for (int index = envelopes.size() - 1; index >= 0; index--) {
            Envelope<K, V> envelope = envelopes.get(index);
            PartitionState current = partitions.get(envelope.partition.topicPartition);
            if (current != null && current.assignmentEpoch == envelope.partition.assignmentEpoch) {
                batch.lane.queue.addFirst(envelope);
            }
        }
    }

    private void complete(Batch<K, V> batch) {
        var affected = new HashSet<PartitionState>();
        for (Envelope<K, V> envelope : batch.envelopes) {
            envelope.offset.completed = true;
            affected.add(envelope.partition);
        }
        for (PartitionState partition : affected) {
            advance(partition);
        }
    }

    private void advance(PartitionState partition) {
        while (!partition.offsets.isEmpty() && partition.offsets.getFirst().completed) {
            partition.lastCompletedLeaderEpoch = partition.offsets.removeFirst().leaderEpoch;
            globalInFlight--;
        }
        OffsetAndMetadata candidate;
        if (partition.offsets.isEmpty()) {
            candidate = partition.observedPosition;
        } else {
            TrackedOffset first = partition.offsets.getFirst();
            candidate = new OffsetAndMetadata(
                    first.offset, partition.lastCompletedLeaderEpoch, "");
        }
        if (candidate != null && candidate.offset() > partition.committedOffset) {
            partition.commitCandidate = candidate;
        }
    }

    private void commitDirty(Consumer<K, V> consumer) {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (PartitionState partition : partitions.values()) {
            if (partition.commitCandidate != null
                    && partition.commitCandidate.offset() > partition.committedOffset) {
                offsets.put(partition.topicPartition, partition.commitCandidate);
            }
        }
        if (offsets.isEmpty()) {
            return;
        }
        consumer.commitSync(offsets);
        for (var committed : offsets.entrySet()) {
            PartitionState partition = partitions.get(committed.getKey());
            if (partition != null && partition.commitCandidate == committed.getValue()) {
                partition.committedOffset = committed.getValue().offset();
            }
        }
    }

    private void applyBackpressure(Consumer<K, V> consumer) {
        boolean globalFull = config.maxInFlightRecords() - globalInFlight < config.maxPollRecords();
        var desired = new HashSet<TopicPartition>();
        for (PartitionState partition : partitions.values()) {
            if (globalFull
                    || config.maxInFlightRecordsPerPartition() - partition.offsets.size()
                            < config.maxPollRecords()) {
                desired.add(partition.topicPartition);
            }
        }
        var resume = new HashSet<>(paused);
        resume.removeAll(desired);
        if (!resume.isEmpty()) {
            consumer.resume(resume);
        }
        var pause = new HashSet<>(desired);
        pause.removeAll(paused);
        if (!pause.isEmpty()) {
            consumer.pause(pause);
        }
        paused.clear();
        paused.addAll(desired);
    }

    private Duration nextPollTimeout() {
        long timeout = pollTimeoutNanos;
        if (activeBatches < config.maxConcurrentBatches()) {
            long now = nanoClock.getAsLong();
            for (Lane<K, V> lane : lanes) {
                if (lane.active == null && !lane.queue.isEmpty()) {
                    timeout = Math.min(timeout, remainingWait(lane.queue.getFirst().observedAt, now));
                }
            }
        }
        return Duration.ofNanos(Math.max(0, timeout));
    }

    private long remainingWait(long observedAt, long now) {
        return Math.max(0, maxBatchWaitNanos - (now - observedAt));
    }

    private void invalidate(Collection<TopicPartition> revoked) {
        var invalidated = new HashSet<PartitionState>();
        for (TopicPartition topicPartition : revoked) {
            PartitionState partition = partitions.remove(topicPartition);
            if (partition != null) {
                invalidated.add(partition);
                globalInFlight -= partition.offsets.size();
                partition.offsets.clear();
            }
            paused.remove(topicPartition);
        }
        if (invalidated.isEmpty()) {
            return;
        }
        for (Lane<K, V> lane : lanes) {
            lane.queue.removeIf(envelope -> invalidated.contains(envelope.partition));
            Batch<K, V> batch = lane.active;
            if (batch != null && touches(batch, invalidated)) {
                batch.invalidated = true;
                batch.thread.interrupt();
            }
        }
    }

    private static <K, V> boolean touches(
            Batch<K, V> batch, Set<PartitionState> invalidated) {
        for (Envelope<K, V> envelope : batch.envelopes) {
            if (invalidated.contains(envelope.partition)) {
                return true;
            }
        }
        return false;
    }

    private void cancelAll() {
        for (Lane<K, V> lane : lanes) {
            lane.queue.clear();
            if (lane.active != null) {
                lane.active.invalidated = true;
                lane.active.thread.interrupt();
                lane.active = null;
            }
        }
        partitions.clear();
        paused.clear();
        completions.clear();
        activeBatches = 0;
        globalInFlight = 0;
    }

    private void wakeOwner() {
        Consumer<K, V> consumer = activeConsumer.get();
        if (consumer == null) {
            return;
        }
        try {
            consumer.wakeup();
        } catch (Throwable failure) {
            if (lifecycle.get() == Lifecycle.RUNNING) {
                asynchronousFailure.compareAndSet(null, failure);
            }
        }
    }

    private void awaitTermination() {
        boolean interrupted = false;
        while (true) {
            try {
                terminated.await();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeDeserializers() {
        var key = keyDeserializer;
        var value = valueDeserializer;
        try (key; value) {
        }
    }

    private static Map<String, Object> validatedProperties(
            Map<String, ?> supplied, ConsumerProcessingConfig config) {
        Objects.requireNonNull(supplied, "properties");
        var properties = new HashMap<String, Object>();
        supplied.forEach((key, value) -> properties.put(
                Objects.requireNonNull(key, "property key"),
                Objects.requireNonNull(value, "property value")));
        Object groupId = properties.get(ConsumerConfig.GROUP_ID_CONFIG);
        if (groupId == null || groupId.toString().isBlank()) {
            throw new IllegalArgumentException("group.id must be present and non-blank");
        }
        Object automaticCommit = properties.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        if (automaticCommit != null && booleanValue(automaticCommit, "enable.auto.commit")) {
            throw new IllegalArgumentException("enable.auto.commit must be false");
        }
        Object maxPollRecords = properties.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        if (maxPollRecords != null
                && integerValue(maxPollRecords, "max.poll.records") != config.maxPollRecords()) {
            throw new IllegalArgumentException(
                    "max.poll.records must match ConsumerProcessingConfig.maxPollRecords");
        }
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords());
        return Map.copyOf(properties);
    }

    private static List<String> validatedTopics(Collection<String> topics) {
        Objects.requireNonNull(topics, "topics");
        var copy = new ArrayList<String>(topics.size());
        for (String topic : topics) {
            Objects.requireNonNull(topic, "topic");
            if (topic.isBlank()) {
                throw new IllegalArgumentException("topics must not contain blank names");
            }
            copy.add(topic);
        }
        if (copy.isEmpty()) {
            throw new IllegalArgumentException("topics must not be empty");
        }
        return List.copyOf(copy);
    }

    private static <K, V> BiConsumer<Consumer<K, V>, ConsumerRebalanceListener> topicSubscription(
            Collection<String> topics) {
        List<String> validated = validatedTopics(topics);
        return (consumer, listener) -> consumer.subscribe(validated, listener);
    }

    private static <K, V> BiConsumer<Consumer<K, V>, ConsumerRebalanceListener> patternSubscription(
            Pattern topicPattern) {
        Objects.requireNonNull(topicPattern, "topicPattern");
        return (consumer, listener) -> consumer.subscribe(topicPattern, listener);
    }

    private static boolean booleanValue(Object value, String name) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String text) {
            String normalized = text.trim();
            if (normalized.equalsIgnoreCase("true") || normalized.equalsIgnoreCase("false")) {
                return Boolean.parseBoolean(normalized);
            }
        }
        throw new IllegalArgumentException(name + " must be true or false");
    }

    private static int integerValue(Object value, String name) {
        if (value instanceof Number number) {
            try {
                return new BigDecimal(number.toString()).intValueExact();
            } catch (NumberFormatException | ArithmeticException failure) {
                throw new IllegalArgumentException(name + " must be an integer", failure);
            }
        }
        if (value instanceof String text) {
            try {
                return Integer.parseInt(text.trim());
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(name + " must be an integer", failure);
            }
        }
        throw new IllegalArgumentException(name + " must be an integer");
    }

    private static Throwable merge(Throwable first, Throwable second) {
        if (first == null) {
            return second;
        }
        first.addSuppressed(second);
        return first;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure == null) {
            return;
        }
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new AssertionError(failure);
    }

    private final class AssignmentListener implements ConsumerRebalanceListener {
        private final Consumer<K, V> consumer;

        private AssignmentListener(Consumer<K, V> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revoked) {
            invalidate(revoked);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> lost) {
            invalidate(lost);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> assigned) {
            for (TopicPartition topicPartition : assigned) {
                partitions.put(
                        topicPartition,
                        new PartitionState(topicPartition, ++assignmentEpoch));
            }
            applyBackpressure(consumer);
        }
    }

    private static final class Lane<K, V> {
        private final int index;
        private final ArrayDeque<Envelope<K, V>> queue = new ArrayDeque<>();
        private Batch<K, V> active;

        private Lane(int index) {
            this.index = index;
        }
    }

    private static final class Envelope<K, V> {
        private final ConsumerRecord<K, V> record;
        private final long observedAt;
        private final PartitionState partition;
        private final TrackedOffset offset;

        private Envelope(
                ConsumerRecord<K, V> record,
                long observedAt,
                PartitionState partition,
                TrackedOffset offset) {
            this.record = record;
            this.observedAt = observedAt;
            this.partition = partition;
            this.offset = offset;
        }
    }

    private static final class Batch<K, V> {
        private final Lane<K, V> lane;
        private final List<Envelope<K, V>> envelopes;
        private Thread thread;
        private boolean invalidated;

        private Batch(Lane<K, V> lane, List<Envelope<K, V>> envelopes) {
            this.lane = lane;
            this.envelopes = envelopes;
        }
    }

    private static final class Completion<K, V> {
        private final Batch<K, V> batch;
        private final Throwable failure;

        private Completion(Batch<K, V> batch, Throwable failure) {
            this.batch = batch;
            this.failure = failure;
        }
    }

    private static final class PartitionState {
        private final TopicPartition topicPartition;
        private final long assignmentEpoch;
        private final ArrayDeque<TrackedOffset> offsets = new ArrayDeque<>();
        private long lastObservedOffset = -1;
        private long committedOffset = -1;
        private Optional<Integer> lastCompletedLeaderEpoch = Optional.empty();
        private OffsetAndMetadata observedPosition;
        private OffsetAndMetadata commitCandidate;

        private PartitionState(TopicPartition topicPartition, long assignmentEpoch) {
            this.topicPartition = topicPartition;
            this.assignmentEpoch = assignmentEpoch;
        }
    }

    private static final class TrackedOffset {
        private final long offset;
        private final Optional<Integer> leaderEpoch;
        private boolean completed;

        private TrackedOffset(long offset, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.leaderEpoch = leaderEpoch;
        }
    }
}
