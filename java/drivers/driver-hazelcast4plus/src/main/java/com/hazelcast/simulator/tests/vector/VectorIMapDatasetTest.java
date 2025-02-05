package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorIMapDatasetTest extends VectorCollectionDatasetTestBase {

    public int putBatchSize = 2_000;

    // IMap parameters
    public String inMemoryFormat;
    public String indexField;

    // search parameters
    public int numberOfSearchIterations = Integer.MAX_VALUE;
    public int limit;
    // negative value means filtering disabled. Allows to pass 1.0 to test overhead of predicate application
    public double matchingEntriesFraction = -1;
    private int numberOfMatchingEntries = -1;

    private final AtomicInteger counter = new AtomicInteger(0);

    private IMap<Integer, VectorDocument<Integer>> collection;

    @Setup
    public void setupIMap() {
        var mapConfig = new MapConfig(collectionName)
                .setInMemoryFormat(inMemoryFormat != null ? InMemoryFormat.valueOf(inMemoryFormat) : MapConfig.DEFAULT_IN_MEMORY_FORMAT)
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount);
        if (indexField != null) {
            mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, indexField).setName("metadata-index"));
        }

        targetInstance.getConfig().addMapConfig(mapConfig);

        collection = targetInstance.getMap(collectionName);

        if (matchingEntriesFraction >= 0) {
            numberOfMatchingEntries = (int) (getRequestedSize() * matchingEntriesFraction);
            logger.info("Will use predicate with {} matching entries.", numberOfMatchingEntries);
        }
    }

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
    public void search() {
        var iteration = counter.getAndIncrement();
        if (iteration >= numberOfSearchIterations) {
            testContext.stop();
            return;
        }
        var vector = testDataset.getSearchVector(iteration % testDataset.size());

        Predicate<Integer, VectorDocument<Integer>> predicate;
        int startId;
        int endId;
        if (numberOfMatchingEntries >= 0) {
            int maxStartId = getRequestedSize() - numberOfMatchingEntries;
            startId = maxStartId > 0 ? ThreadLocalRandom.current().nextInt(maxStartId) : maxStartId;
            endId = startId + numberOfMatchingEntries - 1;
            // between is inclusive
            // use indexed field if available, otherwise filter on value
            predicate = Predicates.between(indexField != null ? indexField : "this.value", startId, endId);
        } else {
            startId = 0;
            endId = 0;
            predicate = null;
        }

        // TODO: handle cases with fetching vectors and values (projection?)
        var result = collection.keySet(com.hazelcast.vector.VectorPredicates.<Integer, VectorDocument<Integer>>nearestNeighbours(limit)
                .to(vector)
                .withEmbedding("vectors")
                .withMetric(Metric.valueOf(metric))
                .matching(predicate)
                .build());
//        var result = collection.keySet(Predicates.pagingPredicate(predicate, limit));

        // sanity checks
        if (result.size() != limit) {
            throw new AssertionError("Expected " + limit + " vectors but got " + result.size());
        }
        if (numberOfMatchingEntries >= 0 && result.stream().anyMatch(i -> i < startId || i > endId)) {
            throw new AssertionError("Expected keys between " + startId + " and " + endId + " but got " + result);
        }
    }

}
