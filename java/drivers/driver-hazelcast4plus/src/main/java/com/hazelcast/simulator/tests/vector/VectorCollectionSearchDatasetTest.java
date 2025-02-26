package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.core.Pipelining;
import com.hazelcast.query.Predicate;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchOptionsBuilder;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.Hints;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class VectorCollectionSearchDatasetTest extends VectorCollectionDatasetTestBase {

    // search parameters
    public int numberOfSearchIterations = Integer.MAX_VALUE;

    public int limit;
    public boolean includeVectors = true;
    public boolean includeValue = true;
    public boolean singleStage = false;
    public Integer efSearch = null;

    // inner test parameters

    private final Queue<TestSearchResult> searchResults = new ConcurrentLinkedQueue<>();

    private final ScoreMetrics scoreMetrics = new ScoreMetrics();

    private SearchOptions options;

    private long indexBuildTime = 0;
    // for inflated collection precision calculation are wrong due to duplicated vectors
    private boolean collectionInflated;

    private final AtomicInteger counter = new AtomicInteger(0);

    @Setup
    public void setupSearch() {
        scoreMetrics.setName(name);
        SearchOptionsBuilder optionsBuilder = SearchOptions.builder()
                .setIncludeValue(includeValue)
                .setIncludeVectors(includeVectors)
                .limit(limit);
        if (efSearch != null) {
            optionsBuilder.hint(Hints.EF_SEARCH, efSearch);
        }
        if (singleStage) {
            optionsBuilder.hint(Hints.FORCE_SINGLE_STAGE_SEARCH, true);
        }
        options = optionsBuilder.build();
    }

    @Prepare(global = true)
    public void prepare() {
        int testDataSetSize = reader.getSize();
        var size = getRequestedSize();
        collectionInflated = size > testDataSetSize;

        if (collection.size() == size) {
            logger.info("Collection seems to be already filled - reusing existing data.");
            // reader will no longer be needed
            reader = null;
            return;
        }

        var indexBuildTimeStart = System.currentTimeMillis();

        Map<Integer, VectorDocument<Integer>> buffer = new HashMap<>();
        Pipelining<Void> pipelining = new Pipelining<>(MAX_PUT_ALL_IN_FLIGHT);
        logger.info("Start loading data...");

        int index = 0;
        while (index < size) {
            buffer.put(index, VectorDocument.of(index % testDataSetSize, VectorValues.of(reader.getTrainVector(index % testDataSetSize))));
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

        logger.info("Collection size: {}", collection.size());
        if (testDataSetSize != size) {
            logger.info("Test dataset size: {}", testDataSetSize);
            if (collectionInflated) {
                logger.warn("Collection was inflated, precision calculation can be wrong");
            }
        }
        logger.info("Collection dimension: {}", reader.getDimension());
        logger.info("Cleanup time: {}s", MILLISECONDS.toSeconds(cleanupTimer));
        logger.info("Index build time: {}s", MILLISECONDS.toSeconds(indexBuildTime));

        // reader will no longer be needed
        reader = null;
    }

    @TimeStep
    public void search() {
        var iteration = counter.getAndIncrement();
        if (iteration >= numberOfSearchIterations) {
            testContext.stop();
            return;
        }
        var vector = testDataset.getSearchVector(iteration % testDataset.size());

        SearchOptions effectiveOptions;
        if (hasFilter()) {
            var filter = createFilter();
            effectiveOptions = options.toBuilder().predicate(filter.predicate()).build();
        } else {
            effectiveOptions = options;
        }

        var result = collection.searchAsync(
                VectorValues.of(vector),
                effectiveOptions
        ).toCompletableFuture().join();
        if (iteration < testDataset.size()) {
            searchResults.add(new TestSearchResult(iteration, vector, effectiveOptions.getPredicate(), result));
        }
    }

    @Teardown(global = true)
    public void afterRun() {
        var evaluationTimeMs = withTimer(() -> searchResults.forEach(testSearchResult -> {
            int index = testSearchResult.index();
            List<Integer> ids = new ArrayList<>();
            VectorUtils.forEach(testSearchResult.results, r -> ids.add((Integer) r.getKey()));

            if (testSearchResult.predicate == null) {
                // use ground truth from the dataset
                scoreMetrics.set((int) (testDataset.getPrecision(ids, index, limit) * 100));
            } else {
                // evaluate ground truth on the collection
                List<Integer> gtIds = new ArrayList<>();
                var gtOpts = SearchOptions.builder()
                        .limit(limit)
                        // get 100% accurate results
                        .hint(Hints.PARTITION_LIMIT, limit)
                        .hint(Hints.USE_FULL_SCAN, true)
                        .predicate(testSearchResult.predicate).build();
                var gtResults = collection.searchAsync(VectorValues.of(testSearchResult.searchVector), gtOpts).toCompletableFuture().join();
                VectorUtils.forEach(gtResults, r -> gtIds.add((Integer) r.getKey()));

                scoreMetrics.set((int) (TestDataset.getPrecision(ids, limit, gtIds.stream().mapToInt(i -> i).toArray()) * 100));
            }
        }));

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
        logger.info("Total results: {}", scoreMetrics.getTotalCount());
        logger.info("Evaluation took: {} ms", evaluationTimeMs);

        searchResults.clear();
    }

    public record TestSearchResult(int index, float[] searchVector,
                                   Predicate<?, ?> predicate,
                                   SearchResults<?, ?> results) {
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

}
