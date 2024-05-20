package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class VectorCollectionPutDatasetTest extends HazelcastTest {

    public String datasetUrl;

    public String workingDirectory;

    // common parameters
    public int loadFirst = Integer.MAX_VALUE;

    public int putBatchSize = 10_000;

    // graph parameters
    public String metric = "COSINE";

    public int maxDegree = 40;

    public int efConstruction = 50;

    // inner test parameters

    private static final String collectionName = "performance-collection";

    private static final TimeMetrics metrics = new TimeMetrics();
    private VectorCollection<Integer, Integer> collection;

    private final AtomicInteger counter = new AtomicInteger(0);

    private DatasetReader reader;
    private List<Map<Integer, VectorDocument<Integer>>> buffers = new ArrayList<>();

    @Setup
    public void setup() {
        reader = DatasetReader.create(datasetUrl, workingDirectory, false);
        int dimension = reader.getDimension();
        loadFirst = Math.min(loadFirst, reader.getSize());

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
    }

    @TimeStep(prob = 0)
    public void put(ThreadState state) {
        var iteration = counter.getAndIncrement();
        if (iteration >= loadFirst) {
            testContext.stop();
            return;
        }
        var vector = reader.getTrainVector(iteration);
        metrics.recordPut(
                () -> collection.putAsync(
                                iteration,
                                VectorDocument.of(iteration, VectorValues.of(vector))
                        )
                        .toCompletableFuture()
                        .join()
        );
    }

    @TimeStep(prob = 1)
    public void putAll(ThreadState state) {
        var iteration = counter.getAndAdd(putBatchSize);
        if (iteration >= loadFirst) {
            testContext.stop();
            return;
        }
        Map<Integer, VectorDocument<Integer>> buffer = new HashMap<>();
        metrics.recordBuffer(
                () -> {
                    for (int i = 0; i < putBatchSize; i++) {
                        var key = iteration + i;
                        if (key >= reader.size) {
                            break;
                        }
                        var vector = reader.getTrainVector(key);
                        buffer.put(key, VectorDocument.of(key, VectorValues.of(vector)));
                    }
                }
        );

        metrics.recordPut(
                () -> collection.putAllAsync(buffer)
                        .toCompletableFuture()
                        .join()
        );
    }

    @AfterRun
    public void afterRun() {
        logger.info("****CUSTOM STATISTICS****");
        logger.info(metrics.getStatistics());
    }

    public static class ThreadState extends BaseThreadState {
    }


    public static class TimeMetrics {
        private static final Histogram bufferTimer = new Histogram(2);
        private static final Histogram putTimer = new Histogram(2);


        public void recordBuffer(Runnable action) {
            var start = System.currentTimeMillis();
            action.run();
            bufferTimer.recordValue(System.currentTimeMillis() - start);
        }

        public void recordPut(Runnable action) {
            var start = System.currentTimeMillis();
            action.run();
            putTimer.recordValue(System.currentTimeMillis() - start);
        }

        public String getStatistics() {
            return "\nBuffer 95p: " + bufferTimer.getValueAtPercentile(95) + "\n"
                    + "Buffer max: " + bufferTimer.getMaxValue() + "\n"
                    + "Buffer min: " + bufferTimer.getMinValue() + "\n"
                    + "Buffer mean: " + bufferTimer.getMean() + "\n"
                    + "Put 95p: " + putTimer.getValueAtPercentile(95) + "\n"
                    + "Put max: " + putTimer.getMaxValue() + "\n"
                    + "Put min: " + putTimer.getMinValue() + "\n"
                    + "Put mean: " + putTimer.getMean() + "\n";
        }
    }
}
