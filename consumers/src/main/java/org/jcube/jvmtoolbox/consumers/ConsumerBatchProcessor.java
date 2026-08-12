package org.jcube.jvmtoolbox.consumers;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Processes one ordered batch from a logical lane.
 *
 * <p>The input list is ordered and unmodifiable. Invocations for the same lane never overlap, while
 * invocations for different lanes may overlap up to the configured limit. Returning marks the whole
 * batch successful; throwing fails the consumer setup unless the batch was invalidated by partition
 * revocation. Partial success is not represented.
 *
 * <p>Interruption requests cancellation but cannot guarantee that downstream effects stop. Processors
 * should use bounded downstream calls and tolerate replay. The consumer does not own or close resources
 * captured by the processor.
 *
 * @param <K> Kafka key type
 * @param <V> Kafka value type
 */
@FunctionalInterface
public interface ConsumerBatchProcessor<K, V> {
    /**
     * Processes one ordered, non-empty batch.
     *
     * @param records ordered, unmodifiable records
     * @throws Exception to fail the active consumer setup
     */
    void process(List<ConsumerRecord<K, V>> records) throws Exception;
}
