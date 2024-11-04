package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Pipelining;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
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

    public String name;

    public String datasetUrl;
    public String testDatasetUrl;

    public String workingDirectory;

    // common parameters
    public int numberOfSearchIterations = Integer.MAX_VALUE;

    public int loadFirst = Integer.MAX_VALUE;

    // graph parameters
    public String metric;

    public int maxDegree;

    public int efConstruction;

    public boolean normalize = false;

    // search parameters

    public int limit;

    // inner test parameters

    private static final int PUT_BATCH_SIZE = 2_000;

    private static final int MAX_PUT_ALL_IN_FLIGHT = 24;

    private VectorCollection<Integer, Integer> collection;

    private DatasetReader reader;
    private DatasetReader testReader;

    private TestDataset testDataset;

    private final Queue<TestSearchResult> searchResults = new ConcurrentLinkedQueue<>();

    private final ScoreMetrics scoreMetrics = new ScoreMetrics();

    private long indexBuildTime = 0;
    private SearchOptions options;

    private final AtomicInteger counter = new AtomicInteger(0);


    @Setup
    public void setup() {
        scoreMetrics.setName(name);
        reader = DatasetReader.create(datasetUrl, workingDirectory, normalize);
        if (testDatasetUrl != null) {
            testReader = DatasetReader.create(testDatasetUrl, workingDirectory, normalize, true);
        }

        int dimension = reader.getDimension();
        assert dimension == reader.getTestDatasetDimension() : "dataset dimension does not correspond to query vector dimension";
        if (testDatasetUrl != null) {
            testDataset = testReader.getTestDataset();
        } else {
            testDataset = reader.getTestDataset();
        }

        logger.info("Vector collection name: {}", name);
        logger.info("Use normalize: {}", normalize);
        collection = VectorCollection.getCollection(
                targetInstance,
                new VectorCollectionConfig(name)
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setMetric(Metric.valueOf(metric))
                                        .setDimension(dimension)
                                        .setMaxDegree(maxDegree)
                                        .setEfConstruction(efConstruction)
                        )
        );

        options = new SearchOptionsBuilder()
                .includeValue()
                .includeVectors()
                .limit(limit)
                .build();
    }

    @Prepare(global = true)
    public void prepare() {
        var size = Math.min(reader.getSize(), loadFirst);

        var indexBuildTimeStart = System.currentTimeMillis();

        Map<Integer, VectorDocument<Integer>> buffer = new HashMap<>();
        Pipelining<Void> pipelining = new Pipelining<>(MAX_PUT_ALL_IN_FLIGHT);
        logger.info("Start loading data...");

        int index = 0;
        while (index < size) {
            buffer.put(index, VectorDocument.of(index, VectorValues.of(reader.getTrainVector(index))));
            index++;
            if (buffer.size() % PUT_BATCH_SIZE == 0) {
                addToPipelineWithLogging(pipelining, collection.putAllAsync(buffer));
                logger.info(
                        "Uploaded {} vectors from {}. Block size: {}. Total time (min): {}",
                        index,
                        size,
                        buffer.size(),
                        MILLISECONDS.toMinutes(System.currentTimeMillis() - indexBuildTimeStart)
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
        try {
            pipelining.results();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        var cleanupTimer = withTimer(() -> collection.optimizeAsync().toCompletableFuture().join());
        indexBuildTime = System.currentTimeMillis() - indexBuildTimeStart;

        logger.info("Collection size: {}", size);
        logger.info("Collection dimension: {}", reader.getDimension());
        logger.info("Cleanup time (min): {}", MILLISECONDS.toMinutes(cleanupTimer));
        logger.info("Index build time (min): {}", MILLISECONDS.toMinutes(indexBuildTime));
    }

    @TimeStep
    public void search() {
        var iteration = counter.getAndIncrement();
        if (iteration >= numberOfSearchIterations) {
            testContext.stop();
            return;
        }
        var vector = testDataset.getSearchVector(iteration % testDataset.size());

        var result = collection.searchAsync(
                VectorValues.of(vector),
                options
        ).toCompletableFuture().join();
        if (iteration < testDataset.size()) {
            searchResults.add(new TestSearchResult(iteration, vector, result));
        }
    }

    @Teardown(global = true)
    public void afterRun() {
        searchResults.forEach(testSearchResult -> {
            int index = testSearchResult.index();
            List<Integer> ids = new ArrayList<>();
            VectorUtils.forEach(testSearchResult.results, r -> ids.add((Integer) r.getKey()));
            scoreMetrics.set((int) (testDataset.getPrecision(ids, index, limit) * 100));
        });

        writeAllSearchResultsToFile("precision_" + name + ".out");
        appendStatisticsToFile();
        logger.info("Results for {}", name);
        logger.info("Min score: {}", scoreMetrics.getMin());
        logger.info("Max score: {}", scoreMetrics.getMax());
        logger.info("Mean score: {}", scoreMetrics.getMean());
        logger.info("5pt: {}", scoreMetrics.getPercentile(5));
        logger.info("10pt: {}", scoreMetrics.getPercentile(10));
        logger.info("The percentage of results with precision lower than 98%: {}", scoreMetrics.getPercentLowerThen(98));
        logger.info("The percentage of results with precision lower than 99%: {}", scoreMetrics.getPercentLowerThen(99));
    }

    public record TestSearchResult(int index, float[] searchVector, SearchResults<?, ?> results) {
    }

    private void writeAllSearchResultsToFile(String fileName) {
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

    private void appendStatisticsToFile() {
        try {
            FileWriter fileWriter = new FileWriter("statistics.out", true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            List<String> values = List.of(
                    name,
                    String.valueOf(indexBuildTime),
                    String.valueOf(scoreMetrics.getMean())
            );
            printWriter.println(String.join(", ", values));
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
        if (v == null || v.vector().length == 0) {
            return 0;
        }
        return v.vector()[0];
    }
}
