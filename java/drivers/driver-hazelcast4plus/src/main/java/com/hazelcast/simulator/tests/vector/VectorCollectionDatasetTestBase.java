package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Pipelining;
import com.hazelcast.function.ThrowingRunnable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base class for vector collection tests using a predefined data set
 */
public class VectorCollectionDatasetTestBase extends HazelcastTest {
    protected static final int PUT_BATCH_SIZE = 2_000;
    protected static final int MAX_PUT_ALL_IN_FLIGHT = 24;
    private static final Filter NO_FILTER = new Filter(null, 0, 0);

    //region dataset parameters
    public String datasetUrl;
    public String testDatasetUrl;
    public String workingDirectory;

    // allows inflating the collection to arbitrary size by repeatedly adding the entries.
    // if smaller than size of the dataset, loads only a subset of it
    public int targetCollectionSize = -1;
    // fraction of the target collection size to be used in given test
    public double targetCollectionSizeFraction = 1.0;

    // if vectors should be normalized before use
    public boolean normalize = false;
    //endregion

    //region collection parameters
    public String collectionName;
    // by default do not use backups to get faster upload
    public int backupCount = 0;
    public int asyncBackupCount = 0;
    //endregion

    //region graph parameters
    public String metric;
    public int maxDegree = VectorIndexConfig.DEFAULT_MAX_DEGREE;
    public int efConstruction = VectorIndexConfig.DEFAULT_EF_CONSTRUCTION;
    public boolean useDeduplication = VectorIndexConfig.DEFAULT_USE_DEDUPLICATION;
    //endregion

    //region search parameters
    public String indexField;
    // negative value means filtering disabled. Allows to pass 1.0 to test overhead of predicate application
    public double matchingEntriesFraction = -1;
    protected int numberOfMatchingEntries = -1;
    //endregion

    //region internal state
    protected DatasetReader reader;
    protected TestDataset testDataset;
    protected VectorCollection<Integer, Integer> collection;
    //endregion

    protected int getRequestedSize() {
        var size = targetCollectionSize > 0 ? targetCollectionSize : reader.getSize();
        return (int) (size * targetCollectionSizeFraction);
    }

    @Setup
    public void setup() {
        if (collectionName == null) {
            collectionName = name;
        }

        reader = DatasetReader.create(datasetUrl, workingDirectory, normalize);

        int dimension = reader.getDimension();
        assert dimension == reader.getTestDatasetDimension() : "dataset dimension does not correspond to query vector dimension";
        if (testDatasetUrl != null) {
            var testReader = DatasetReader.create(testDatasetUrl, workingDirectory, normalize, true);
            testDataset = testReader.getTestDataset();
        } else {
            testDataset = reader.getTestDataset();
        }

        logger.info("Vector collection name: {}", collectionName);
        logger.info("Use normalize: {}", normalize);
        collection = VectorCollection.getCollection(
                targetInstance,
                new VectorCollectionConfig(collectionName)
                        .setBackupCount(backupCount)
                        .setAsyncBackupCount(asyncBackupCount)
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setMetric(Metric.valueOf(metric))
                                        .setDimension(dimension)
                                        .setMaxDegree(maxDegree)
                                        .setEfConstruction(efConstruction)
                                        .setUseDeduplication(useDeduplication)
                        )
        );

        if (matchingEntriesFraction >= 0) {
            numberOfMatchingEntries = (int) (getRequestedSize() * matchingEntriesFraction);
            logger.info("Will use predicate with {} matching entries.", numberOfMatchingEntries);
        }

    }

    @Teardown
    public void freeDataset() {
        // test dataset will no longer be needed, free it to save memory as the test instance is kept longer
        testDataset = null;
        reader = null;
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

    protected Filter createFilter() {
        Predicate<Integer, VectorDocument<Integer>> predicate;
        if (hasFilter()) {
            int maxStartId = getRequestedSize() - numberOfMatchingEntries;
            int startId = maxStartId > 0 ? ThreadLocalRandom.current().nextInt(maxStartId) : maxStartId;
            int endId = startId + numberOfMatchingEntries - 1;
            // between is inclusive
            // use indexed field if available, otherwise filter on value
            predicate = Predicates.between(indexField != null ? indexField : "this.value", startId, endId);
            return new Filter(predicate, startId, endId);
        } else {
            return NO_FILTER;
        }
    }

    protected boolean hasFilter() {
        return numberOfMatchingEntries >= 0;
    }

    protected record Filter(Predicate<Integer, VectorDocument<Integer>> predicate, int startId, int endId) {
    }

    protected static long withTimer(ThrowingRunnable runnable) {
        var start = System.currentTimeMillis();
        runnable.run();
        return System.currentTimeMillis() - start;
    }

    protected static float getFirstCoordinate(VectorValues vectorValues) {
        var v = (VectorValues.SingleVectorValues) vectorValues;
        if (v == null || v.vector().length == 0) {
            return 0;
        }
        return v.vector()[0];
    }
}
