package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.Pipelining;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorValues;

import java.util.concurrent.CompletionStage;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Base class for vector collection tests using a predefined data set
 */
public class VectorCollectionDatasetTestBase extends HazelcastTest {
    protected static final int PUT_BATCH_SIZE = 2_000;
    protected static final int MAX_PUT_ALL_IN_FLIGHT = 24;

    //region dataset parameters
    public String datasetUrl;
    public String testDatasetUrl;
    public String workingDirectory;

    // allows inflating the collection to arbitrary size by repeatedly adding the entries.
    // if smaller than size of the dataset, loads only a subset of it
    public int targetCollectionSize = -1;

    // if vectors should be normalized before use
    public boolean normalize = false;
    //endregion

    //region collection parameters
    public String collectionName;
    // by default do not use backups to get faster upload
    public int backupCount = 0;
    //endregion

    //region graph parameters
    public String metric;
    public int maxDegree = VectorIndexConfig.DEFAULT_MAX_DEGREE;
    public int efConstruction = VectorIndexConfig.DEFAULT_EF_CONSTRUCTION;
    public boolean useDeduplication = VectorIndexConfig.DEFAULT_USE_DEDUPLICATION;
    //endregion


    //region internal state
    protected DatasetReader reader;
    protected TestDataset testDataset;
    protected VectorCollection<Integer, Integer> collection;
    //endregion

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
                        .addVectorIndexConfig(
                                new VectorIndexConfig()
                                        .setMetric(Metric.valueOf(metric))
                                        .setDimension(dimension)
                                        .setMaxDegree(maxDegree)
                                        .setEfConstruction(efConstruction)
                                        .setUseDeduplication(useDeduplication)
                        )
        );

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

    protected static long withTimer(Runnable runnable) {
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
