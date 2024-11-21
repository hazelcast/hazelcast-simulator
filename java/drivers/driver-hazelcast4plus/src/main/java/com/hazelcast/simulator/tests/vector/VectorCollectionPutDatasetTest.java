package com.hazelcast.simulator.tests.vector;

import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorCollectionPutDatasetTest extends VectorCollectionDatasetTestBase {

    public int putBatchSize = 2_000;

    private final AtomicInteger counter = new AtomicInteger(0);

    @TimeStep(prob = 0)
    public void put() {
        int testDataSetSize = reader.getSize();
        var size = getRequestedSize();

        var index = counter.getAndIncrement();
        if (index >= size) {
            testContext.stop();
            return;
        }
        var vector = reader.getTrainVector(index % testDataSetSize);
        collection.putAsync(index, VectorDocument.of(index % testDataSetSize, VectorValues.of(vector)))
                .toCompletableFuture()
                .join();
    }

    @TimeStep(prob = 0)
    public void putAll() {
        int testDataSetSize = reader.getSize();
        var size = getRequestedSize();

        var iteration = counter.getAndAdd(putBatchSize);
        if (iteration >= size) {
            testContext.stop();
            return;
        }
        Map<Integer, VectorDocument<Integer>> buffer = new HashMap<>();
        for (int i = 0; i < putBatchSize; i++) {
            var key = iteration + i;
            if (key >= testDataSetSize) {
                break;
            }
            var vector = reader.getTrainVector(key % testDataSetSize);
            buffer.put(key, VectorDocument.of(key, VectorValues.of(vector)));
        }

        collection.putAllAsync(buffer)
                .toCompletableFuture()
                .join();
    }

    /**
     * Deletes entries 0..requested size
     */
    @TimeStep(prob = 0)
    public void delete() {
        var size = getRequestedSize();

        var index = counter.getAndIncrement();
        if (index >= size) {
            testContext.stop();
            return;
        }
        collection.deleteAsync(index)
                .toCompletableFuture()
                .join();
    }

    @TimeStep(prob = 0)
    public void optimize() {
        // note: this may fail with > 1 client
        var cleanupTimer = withTimer(() -> collection.optimizeAsync().toCompletableFuture().join());
        logger.info("Cleanup time: {} ms", cleanupTimer);
        testContext.stop();
    }
}
