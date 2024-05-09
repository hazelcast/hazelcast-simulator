package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Pipelining;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchOptionsBuilder;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    private static final int MAX_PUT_ALL_IN_FLIGHT = 5;

    private VectorCollection<Integer, Integer> collection;

    private final AtomicInteger searchCounter = new AtomicInteger(0);

    private final AtomicInteger putCounter = new AtomicInteger(0);

    private TestDataset testDataset;

    private final Queue<TestSearchResult> searchResults = new ConcurrentLinkedQueue<>();

    @Setup
    public void setup() {
        DatasetReader reader = DatasetReader.create(datasetUrl, workingDirectory);
        var size = Math.min(reader.getSize(), loadFirst);
        int dimension = reader.getDimension();
        assert dimension == reader.getTestDatasetDimension() : "dataset dimension does not correspond to query vector dimension";
        testDataset = reader.getTestDataset();
        numberOfSearchIterations = Math.min(numberOfSearchIterations, testDataset.size());

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
        Pipelining<Void> pipelining = new Pipelining<>(MAX_PUT_ALL_IN_FLIGHT);
        logger.info("Start loading data...");

        while ((index = putCounter.getAndIncrement()) < size) {
            buffer.put(index, VectorDocument.of(index, VectorValues.of(reader.getTrainVector(index))));
            if (buffer.size() % PUT_BATCH_SIZE == 0) {
                addToPipelineWithLogging(pipelining, collection.putAllAsync(buffer));
                logger.info(
                        "Uploaded {} vectors from {}. Block size: {}. Total time (min): {}",
                        index,
                        size,
                        buffer.size(),
                        MILLISECONDS.toMinutes(System.currentTimeMillis() - start)
                );
                buffer = new HashMap<>();
            }
        }
        if (!buffer.isEmpty()) {
            addToPipelineWithLogging(pipelining, collection.putAllAsync(buffer));
            logger.info("Uploading last vectors block. Last block size: {}.", buffer.size());
            buffer.clear();
        }

        logger.info("Start waiting pipeline results...");
        var pipelineWaiting = withTimer(() -> {
            try {
                pipelining.results();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        logger.info("Pipeline waiting finished in {} min", MILLISECONDS.toMinutes(pipelineWaiting));

        var cleanupTimer = withTimer(() -> collection.optimizeAsync().toCompletableFuture().join());

        logger.info("Collection size: {}", size);
        logger.info("Collection dimension: {}", reader.getDimension());
        logger.info("Cleanup time (min): {}", MILLISECONDS.toMinutes(cleanupTimer));
        logger.info("Index build time (min): {}", MILLISECONDS.toMinutes(System.currentTimeMillis() - start));
    }

    @TimeStep()
    public void search(ThreadState state) {
        var iteration = searchCounter.getAndIncrement();
        if (iteration >= numberOfSearchIterations) {
            testContext.stop();
            return;
        }
        var vector = testDataset.getSearchVector(iteration);
        SearchOptions options = new SearchOptionsBuilder().vector(vector).includePayload().includeVectors().limit(limit).build();
        var result = collection.searchAsync(options).toCompletableFuture().join();
        searchResults.add(new TestSearchResult(iteration, vector, result));
    }

    @AfterRun
    public void afterRun() {
        searchResults.forEach(testSearchResult -> {
            int index = testSearchResult.index();
            List<Integer> ids = new ArrayList<>();
            VectorUtils.forEach(testSearchResult.results, r -> ids.add((Integer) r.getKey()));
            ScoreMetrics.set((int) (testDataset.getPrecisionV1(ids, index, limit) * 100));
        });

        writePureResultsToFile("precision.out");
        logger.info("Number of search iteration: {}", searchCounter.get());
        logger.info("Min score: {}", ScoreMetrics.getMin());
        logger.info("Max score: {}", ScoreMetrics.getMax());
        logger.info("Mean score: {}", ScoreMetrics.getMean());
        logger.info("Percent of results lower then 98% precision: {}", ScoreMetrics.getPercentLowerThen(98));
    }

    public static class ThreadState extends BaseThreadState {
    }

    public record TestSearchResult(int index, float[] searchVector, SearchResults results) {
    }

    private void writePureResultsToFile(String fileName) {
        try {
            Function<Float, Float> restore = VectorUtils.restoreRealMetric(Metric.valueOf(metric));
            var fileWriter = new FileWriter(fileName);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println("index, searchVector0, foundVector0, foundVectorKey, foundVectorScore, restoredRealVectorScore");
            searchResults.forEach(
                    testSearchResult -> VectorUtils.forEach(
                            testSearchResult.results,
                            (result) -> printWriter.printf(
                                    "%d, %s, %s, %s, %s, %s\n",
                                    testSearchResult.index,
                                    testSearchResult.searchVector[0],
                                    getFirstCoordinate(result.getVectors()),
                                    result.getKey(),
                                    result.getScore(),
                                    restore.apply(result.getScore())
                            )
                    )
            );
            printWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void addToPipelineWithLogging(Pipelining<Void> pipelining, CompletionStage<Void> asyncInvocation) {
        var now = System.currentTimeMillis();
        try {
            pipelining.add(asyncInvocation);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        var msBlocked = System.currentTimeMillis() - now;
        // log if we were blocked for more than 30 sec
        if (msBlocked > 30_000) {
            logger.info(
                    "Thread was blocked for {} sec due to reaching max pipeline depth",
                    MILLISECONDS.toSeconds(msBlocked)
            );
        }
    }

    private long withTimer(Runnable runnable) {
        var start = System.currentTimeMillis();
        runnable.run();
        return System.currentTimeMillis() - start;
    }

    private float getFirstCoordinate(VectorValues vectorValues) {
        var v = (VectorValues.SingleVectorValues) vectorValues;
        if(v == null || v.vector().length == 0) {
            return 0;
        }
        return v.vector()[0];
    }


}
