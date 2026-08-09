package org.jcube.jvmtoolbox.batching;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class BenchmarkSupport {
    static final int VALUE = 42;

    private BenchmarkSupport() {}

    static BatchingConfig config(
            int batchSize,
            Duration maxWait,
            int concurrentBatches,
            int capacity,
            AdmissionPolicy admissionPolicy,
            Duration admissionTimeout) {
        return new BatchingConfig(
                batchSize,
                maxWait,
                concurrentBatches,
                capacity,
                admissionPolicy,
                admissionTimeout);
    }

    static List<BatchOutcome<Integer>> integerOutcomes(int size) {
        var outcomes = new ArrayList<BatchOutcome<Integer>>(size);
        for (int index = 0; index < size; index++) {
            outcomes.add(BatchOutcome.success(VALUE));
        }
        return List.copyOf(outcomes);
    }
}
