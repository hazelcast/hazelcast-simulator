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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class VectorCollectionSearchDatasetTest extends HazelcastTest {

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

    private final AtomicInteger searchCounter = new AtomicInteger(0);

    private final AtomicInteger putCounter = new AtomicInteger(0);
    private float[][] testDataset;

    @Setup
    public void setup() {
        DatasetReader reader = DatasetReader.create(datasetUrl, workingDirectory);
        var size = Math.min(reader.getSize(), loadFirst);
        int dimension = reader.getDimension();
        assert dimension == reader.getTestDatasetDimension() : "dataset dimension does not correspond to query vector dimension";
        testDataset = reader.getTestDataset();
        numberOfSearchIterations = Math.min(numberOfSearchIterations, testDataset.length);

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

        Map<Integer, VectorDocument<Integer>> buffer = new HashMap<>();
        int index;
        logger.info("Start loading data...");
        while ((index = putCounter.getAndIncrement()) < size) {
            buffer.put(index, VectorDocument.of(index, VectorValues.of(reader.getTrainVector(index))));
            if (buffer.size() % PUT_BATCH_SIZE == 0) {
                var blockStart = System.currentTimeMillis();
                collection.putAllAsync(buffer).toCompletableFuture().join();
                logger.info(
                        format(
                                "Uploaded %s vectors from %s. Block size: %s. Delta (m): %s.  Total time (m): %s",
                                index,
                                size,
                                buffer.size(),
                                TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - blockStart),
                                TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - start)
                        )
                );
                buffer.clear();
            }
        }
        if (!buffer.isEmpty()) {
            collection.putAllAsync(buffer).toCompletableFuture().join();
            logger.info(format("Uploaded vectors. Last block size: %s.", buffer.size()));
            buffer.clear();
        }
        var startCleanup = System.currentTimeMillis();
        collection.optimizeAsync().toCompletableFuture().join();

        logger.info("Collection size: " + size);
        logger.info("Collection dimension: " + reader.getDimension());
        logger.info("Cleanup time(ms): " + (System.currentTimeMillis() - startCleanup));
        logger.info("Index build time(m): " + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - start));
    }

    @TimeStep(prob = 1)
    public void search(ThreadState state) {
        var iteration = searchCounter.incrementAndGet();
        if (iteration >= numberOfSearchIterations) {
            testContext.stop();
            return;
        }
        var vector = testDataset[iteration];
        SearchOptions options = new SearchOptionsBuilder().vector(vector).includePayload().limit(limit).build();
        var result = collection.searchAsync(options).toCompletableFuture().join();

        var score = result.results().next().getScore();
        ScoreMetrics.set((int) (score * 100));
        logger.info("Found score: " + result.results().next().getScore());
    }

    @AfterRun
    public void afterRun() {
        logger.info("Number of search iteration: " + searchCounter.get());
        logger.info("Min score: " + ScoreMetrics.getMin());
        logger.info("Max score: " + ScoreMetrics.getMax());
        logger.info("Mean score: " + ScoreMetrics.getMean());
        logger.info("Percent lower then 0.98: " + ScoreMetrics.getPercentLowerThen(98));
    }

    public static class ThreadState extends BaseThreadState {
    }
}
