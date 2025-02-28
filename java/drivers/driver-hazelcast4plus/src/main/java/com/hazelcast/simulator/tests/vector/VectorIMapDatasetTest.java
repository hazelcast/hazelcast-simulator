package com.hazelcast.simulator.tests.vector;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorPredicates;
import com.hazelcast.vector.VectorValues;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.jetbrains.annotations.NotNull;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorIMapDatasetTest extends VectorCollectionDatasetTestBase {

    public int putBatchSize = 2_000;

    // IMap parameters
    public String inMemoryFormat;
    public boolean useCompactSerialization;
    public String extraIndexFields;
    public String extraIndexType = IndexType.SORTED.name();
    public String cacheDeserializedValues = CacheDeserializedValues.INDEX_ONLY.name();

    // search parameters
    public String searchMode = "entrySet";
    public int numberOfSearchIterations = Integer.MAX_VALUE;
    // -1 means use limit equal to the number of ground truth results
    public int limit = -1;

    private final AtomicInteger counter = new AtomicInteger(0);

    private IMap<Integer, VectorDocument<?>> collection;
    // histogram for collecting extra stats during execution.
    // the meaning depends on the test configuration. It ca be eg.
    // statistics of topK or filter selectivity. Not always used.
    private final Histogram countHistogram = new ConcurrentHistogram(1_000_000, 3);

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
        if (extraIndexFields != null) {
            Arrays.stream(extraIndexFields.split(";")).forEach(field -> {
                // arxiv benchmark: timestamp fields always use sorted index
                var type = field.endsWith("_ts") ? IndexType.SORTED : IndexType.valueOf(extraIndexType);
                mapConfig.addIndexConfig(new IndexConfig(type, field));
            });
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
    private VectorDocument<?> createVectorDocument(int index, int testDataSetSize, float[] vector) {
        Object value = getValue(index % testDataSetSize);

        if (value instanceof HazelcastJsonValue json) {
            // convert JSON to dedicated class to allow filtering to work
            // currently it is not possible to use VectorDocument<HazelcastJsonValue> with predicate API
            // and there are mismatches in serialization (zero-config compact vs java serializable vs IDS)
            // so this seems to be the simplest approach that works.
            // as a bonus we get some reasonable POJO.
            return SerializableArxivVectorDocument.of(json, vector);
        }

        return useCompactSerialization
                ? CompactIntegerVectorDocument.of((Integer) value, VectorValues.of(vector))
                : VectorDocument.of(value, VectorValues.of(vector));
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
        Map<Integer, VectorDocument<?>> buffer = new HashMap<>();
        for (int i = 0; i < putBatchSize; i++) {
            var key = iteration + i;
            if (key >= testDataSetSize) {
                break;
            }
            var vector = reader.getTrainVector(key % testDataSetSize);
            buffer.put(key, createVectorDocument(key, testDataSetSize, vector));
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
        int index = iteration % testDataset.size();
        var vector = testDataset.getSearchVector(index);

        Filter filter;

        if (hasRandomFilter()) {
            filter = createFilter();
        } else {
            var testDatasetFilter = testDataset.getSearchConditions(index);
            filter = testDatasetFilter != null ? new Filter(testDatasetFilter, -1, -1) : NO_FILTER;
        }

        // uncomment to skip cases with low selectivity - update_date_ts filters
//        if (filter.predicate().toString().contains("update_date_ts")) {
//            search();
//            return;
//        }

        switch (searchMode) {
            case "entrySet":
                // TODO: handle cases with fetching keys, vectors and values (projection?)
                int topK = limit > 0 ? limit : testDataset.getLimit(index);
                countHistogram.recordValue(topK);

                Set<Map.Entry<Integer, VectorDocument<?>>> result = collection.entrySet(VectorPredicates.<Integer, VectorDocument<?>>nearestNeighbours(topK)
                        .to(vector)
                        .withEmbedding("vectors")
                        .withMetric(Metric.valueOf(metric))
                        .matching(filter.predicate())
                        .build());

                // sanity checks
                if (result.size() != topK) {
                    throw new AssertionError("Expected " + topK + " vectors but got " + result.size());
                }

                if (hasRandomFilter()) {
                    if (result.stream().mapToInt(Map.Entry::getKey).anyMatch(i -> i < filter.startId() || i > filter.endId())) {
                        throw new AssertionError("Expected keys between " + filter.startId() + " and " + filter.endId() + " but got " + result);
                    }
                } else if (isDefaultSize()) {
                    var precision = testDataset.getPrecision(result.stream().map(Map.Entry::getKey).toList(), index, topK);
                    // it can be a false negative if 2 vectors have the same distance, so use a more relaxed criteria
                    // but ensure that we do not get garbage results
                    if (precision < 0.9) {
                        throw new AssertionError("Expected perfect recall but got " + precision);
                    }
                }
                break;

            case "count":
                // collect stats of predicate selectivity
                // this mode also gives and lower bound on actual vector filtering performance
                // as it only executed structured predicate and count, without distance calculation.
                // this is quite important for interpretation, because PredicateAPI filtering
                // has some non-trivial cost on its own.
                var count = collection.aggregate(Aggregators.count(), filter.predicate());
                countHistogram.recordValue(count);
                break;
        }
    }

    @Teardown(global = true)
    public void afterRun() throws FileNotFoundException {
        if (countHistogram.getTotalCount() > 0) {
            // not use .hdr extension because simulator uses them in report generation
            try (var ps = new PrintStream("count_" + name + ".out")) {
                countHistogram.outputPercentileDistribution(ps, 1.0);
            }
        }
    }
}
