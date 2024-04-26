package com.hazelcast.simulator.tests.vector;

import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchOptionsBuilder;
import com.hazelcast.vector.VectorCollection;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.impl.SingleIndexVectorValues;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Deprecated
public class VectorCollectionRandomTest extends HazelcastTest {
    // common parameters
    public int numberOfIterations = 0;
    public int initialCollectionSize = 0;

    public int minCoordinate = 0;

    public int maxCoordinate = 100;

    public boolean generateOnlyIntCoordinate = false;

    // graph parameters
    public int dimension = 10;

    public String metric = "EUCLIDEAN";

    public int maxDegree = 40;

    public int efConstruction = 50;

    // search parameters

    public int limit = 100;


    // inner test parameters

    private final String collectionName = "performance-collection";
    private VectorCollection<Integer, Integer> collection;

    private final AtomicInteger counter = new AtomicInteger(initialCollectionSize);

    private final Random RANDOM = new Random();
    private long start;

    @Setup
    public void setup() {
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

        for (int i = 0; i < initialCollectionSize; i++) {
            generateVectorAndPutSync(i);
        }
        start = System.currentTimeMillis();
        logger.info("Generated collection size: " + initialCollectionSize);
        logger.info("Start time: " + start);

    }

    @TimeStep
    public void put(ThreadState state) {
        var key = counter.incrementAndGet();
        if(numberOfIterations > 0 && key >= numberOfIterations - initialCollectionSize) {
            testContext.stop();
            return;
        }
        generateVectorAndPutSync(key);
    }

    @TimeStep(prob = 0)
    public void search(ThreadState state) {
        var iteration = counter.incrementAndGet();
        if(numberOfIterations > 0 && iteration >= numberOfIterations) {
            testContext.stop();
            return;
        }
        var vector = generateVector(dimension, 0, maxCoordinate);
        SearchOptions options = new SearchOptionsBuilder().vector(vector).includePayload().limit(limit).build();
        var result = collection.searchAsync(options).toCompletableFuture().join();
        assertEquals(limit, result.size());
    }

    private void generateVectorAndPutSync(int key) {
        collection.putAsync(
                        key,
                        VectorDocument.of(key, VectorValues.of(generateVector(dimension, minCoordinate, maxCoordinate)))
                )
                .toCompletableFuture()
                .join();
    }


    @Verify
    public void verify() {
        logger.info("======== VERIFYING VECTOR COLLECTION =========");
        logger.info("Collection size is: " + counter.get());
        logger.info("Test duration (minutes): " + (System.currentTimeMillis() - start)/60_000);
        int verifySize = counter.get() / 100;
        int[] verifyKeys = RANDOM.ints(verifySize, 0, counter.get()).toArray();
        for (int key : verifyKeys) {
            VectorDocument<Integer> value = collection.getAsync(key).toCompletableFuture().join();
            assertNotNull("No value found for the key: " + key, value);
            var vector = ((SingleIndexVectorValues)value.getVectors()).vector();
            logger.info(key + " - " + Arrays.toString(vector));
        }
    }

    public class ThreadState extends BaseThreadState {
    }

    private float[] generateVector(int length, int minValue, int maxValue) {
        assert minValue >= 0 : "negative min value is not supported";
        Supplier<Float> generator = generateOnlyIntCoordinate ? () -> (float) RANDOM.nextInt(minValue, maxValue) : () -> RANDOM.nextFloat(minValue, maxValue);
        float[] value = new float[length];
        range(0, length).forEach(i -> value[i] = generator.get());
        return value;
    }

}
