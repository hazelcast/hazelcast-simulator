package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchOptionsBuilder;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@Deprecated
public class VectorCollectionFromUrlTest extends HazelcastTest {

    public String datasetUrl;

    public String workingDirectory;

    // common parameters
    public int numberOfSearchIterations = Integer.MAX_VALUE;

    public int loadFirst = Integer.MAX_VALUE;

    // graph parameters
    public String metric = "COSINE";

    public int maxDegree = 40;

    public int efConstruction = 50;

    // search parameters

    public int limit = 1;

    // inner test parameters

    private static final String collectionName = "performance-collection";

    private static final int PUT_BATCH_SIZE = 10_000;
    private VectorCollection<Integer, Integer> collection;

    private final AtomicInteger counter = new AtomicInteger(0);
    private List<float[]> query;

    @Setup
    public void setup() {
        NpyDatasetReader reader = new NpyDatasetReader(datasetUrl, workingDirectory);
        var size = Math.min(reader.getSize(), loadFirst);
        int dimension = reader.getDimension();
        assert dimension == reader.getQueryDimension() : "dataset dimension does not correspond to query vector dimension";
        query = reader.getTestCases();
        numberOfSearchIterations = Math.min(numberOfSearchIterations, query.size());

        collection = VectorCollection.getCollection(
                targetInstance,
                new VectorCollectionConfig(collectionName)
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setMetric(Metric.valueOf(metric))
                                        .setDimension(dimension)
                                        .setMaxDegree(maxDegree)
                                        .setEfConstruction(efConstruction)
                        )
        );

        var start = System.currentTimeMillis();
        for (int i = 0; i < size; i += PUT_BATCH_SIZE) {
            logger.info(
                    String.format(
                            "Uploaded %s vectors from %s. Time: %s",
                            i,
                            size,
                            TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - start)
                    )
            );
            int currentBatchLength = Math.min(PUT_BATCH_SIZE, size - i);
            int[] keys = new int[currentBatchLength];
            float[][] vectors = new float[currentBatchLength][];
            for (int j = 0; j < currentBatchLength; j++) {
                keys[j] = i + j;
                vectors[j] = reader.read(i + j);
            }
            putBatchSync(keys, vectors);
        }
        logger.info("Collection size: " + size);
        logger.info("Collection dimension: " + reader.getDimension());
        logger.info("Index build time(m): " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - start));
    }

    @TimeStep(prob = 0)
    public void search(ThreadState state) {
        var iteration = counter.incrementAndGet();
        if (iteration >= numberOfSearchIterations) {
            testContext.stop();
            return;
        }
        var vector = query.get(iteration);
        SearchOptions options =  new SearchOptionsBuilder().vector(vector).includePayload().limit(limit).build();
        var result = collection.searchAsync(options).toCompletableFuture().join();
        assertEquals(limit, result.size());
        var score = result.results().next().getScore();
        ScoreMetrics.set((int) (score * 100));
        logger.info("Found score: " + result.results().next().getScore());
    }

    @AfterRun
    public void afterRun() {
        logger.info("Number of search iteration: " + counter.get());
        logger.info("Min score: " + ScoreMetrics.getMin());
        logger.info("Max score: " + ScoreMetrics.getMax());
        logger.info("Mean score: " + ScoreMetrics.getMean());
        logger.info("Percent lower then 0.98: " + ScoreMetrics.getPercentLowerThen(98));
    }

    private void putSync(int key, float[] vector) {
        collection.putAsync(
                        key,
                        VectorDocument.of(key, VectorValues.of(vector))
                )
                .toCompletableFuture()
                .join();
    }

    private void putBatchSync(int[] keys, float[][] vectors) {
        CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            futures[i] = collection.putAsync(
                            keys[i],
                            VectorDocument.of(keys[i], VectorValues.of(vectors[i]))
                    )
                    .toCompletableFuture();
        }
        CompletableFuture.allOf(futures).join();
    }


    public static class ThreadState extends BaseThreadState {
    }
}
