package com.hazelcast.simulator.tests.vector;

import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.HazelcastUtils;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorCollectionPutDatasetTest extends VectorCollectionDatasetTestBase {

    public boolean stopAfterOptimize = true;
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
    public void optimize() throws ExecutionException, InterruptedException {
        var start = System.currentTimeMillis();

        // TODO: this will cause redundant invocations with >1 client
        var cleanupTimer = withTimer(() -> collection.optimizeAsync().toCompletableFuture().join());

        if (backupCount + asyncBackupCount > 0) {
            // optimize backups are async and blocking (can be parked) - wait for them also to get true time
            HazelcastUtils.waitForClusterSafeState(targetInstance);
            HazelcastUtils.waitForNoParkedOperations(targetInstance);
            var cleanupTotalTimer = System.currentTimeMillis() - start;
            logger.info("Cleanup primary time: {} ms", cleanupTimer);
            logger.info("Cleanup wait for backups time: {} ms", cleanupTotalTimer - cleanupTimer);
            logger.info("Cleanup total time: {} ms", cleanupTotalTimer);
        } else {
            logger.info("Cleanup time: {} ms", cleanupTimer);
        }

        if (stopAfterOptimize) {
            testContext.stop();
        }
    }

}
