package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorIMapDatasetTest extends VectorCollectionDatasetTestBase {

    public int putBatchSize = 2_000;

    // IMap parameters
    public String inMemoryFormat;
    public boolean useCompactSerialization;
    public String cacheDeserializedValues = CacheDeserializedValues.INDEX_ONLY.name();

    // search parameters
    public int numberOfSearchIterations = Integer.MAX_VALUE;
    public int limit;

    private final AtomicInteger counter = new AtomicInteger(0);

    private IMap<Integer, VectorDocument<Integer>> collection;

    @Setup
    public void setupIMap() {
        var mapConfig = new MapConfig(collectionName)
                .setInMemoryFormat(inMemoryFormat != null ? InMemoryFormat.valueOf(inMemoryFormat) : MapConfig.DEFAULT_IN_MEMORY_FORMAT)
                .setBackupCount(backupCount)
                .setAsyncBackupCount(asyncBackupCount)
                .setCacheDeserializedValues(CacheDeserializedValues.parseString(cacheDeserializedValues));
        if (indexField != null) {
            mapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, indexField).setName("metadata-index"));
        }

        targetInstance.getConfig().addMapConfig(mapConfig);

        collection = targetInstance.getMap(collectionName);
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
        collection.putAsync(index, createVectorDocument(index, testDataSetSize, vector))
                .toCompletableFuture()
                .join();
    }

    @NotNull
    private VectorDocument<Integer> createVectorDocument(int index, int testDataSetSize, float[] vector) {
        return useCompactSerialization
                ? CompactIntegerVectorDocument.of(index % testDataSetSize, VectorValues.of(vector))
                : VectorDocument.of(index % testDataSetSize, VectorValues.of(vector));
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

        var filter = createFilter();

        // TODO: handle cases with fetching keys, vectors and values (projection?)
        var result = collection.entrySet(com.hazelcast.vector.VectorPredicates.<Integer, VectorDocument<Integer>>nearestNeighbours(limit)
                .to(vector)
                .withEmbedding("vectors")
                .withMetric(Metric.valueOf(metric))
                .matching(filter.predicate())
                .build());
//        var result = collection.keySet(Predicates.pagingPredicate(predicate, limit));

        // sanity checks
        if (result.size() != limit) {
            throw new AssertionError("Expected " + limit + " vectors but got " + result.size());
        }
        if (numberOfMatchingEntries >= 0 && result.stream().mapToInt(Map.Entry::getKey).anyMatch(i -> i < filter.startId() || i > filter.endId())) {
            throw new AssertionError("Expected keys between " + filter.startId() + " and " + filter.endId() + " but got " + result);
        }
    }

}
