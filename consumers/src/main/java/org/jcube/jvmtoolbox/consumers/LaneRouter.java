package org.jcube.jvmtoolbox.consumers;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ToIntFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Selects the ordered logical lane for a non-null-key Kafka record.
 *
 * <p>The returned integer is normalized with {@link Math#floorMod(int, int)} against the configured
 * lane count. Equal ordering identities must always produce the same value for the lifetime of a
 * consumer setup.
 *
 * @param <K> Kafka key type
 * @param <V> Kafka value type
 */
@FunctionalInterface
public interface LaneRouter<K, V> {
    /**
     * Returns an integer identifying the record's logical lane.
     *
     * @param record non-null-key record to route
     * @return lane identity before normalization
     */
    int route(ConsumerRecord<K, V> record);

    /**
     * Routes with the key's value-based {@link Object#hashCode()} implementation.
     *
     * <p>Do not use this router for array keys, whose default hash code is identity-based.
     *
     * @param <K> Kafka key type
     * @param <V> Kafka value type
     * @return value-hash router
     */
    static <K, V> LaneRouter<K, V> byKeyHashCode() {
        return record -> record.key().hashCode();
    }

    /**
     * Routes byte-array keys by content.
     *
     * @param <V> Kafka value type
     * @return byte-array-content router
     */
    static <V> LaneRouter<byte[], V> byByteArrayKey() {
        return record -> Arrays.hashCode(record.key());
    }

    /**
     * Adapts an application-defined key hash.
     *
     * @param keyHash deterministic hash of the ordering identity
     * @param <K> Kafka key type
     * @param <V> Kafka value type
     * @return application-key router
     */
    static <K, V> LaneRouter<K, V> byKey(ToIntFunction<? super K> keyHash) {
        Objects.requireNonNull(keyHash, "keyHash");
        return record -> keyHash.applyAsInt(record.key());
    }
}
