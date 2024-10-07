package com.hazelcast.simulator.hz;

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

public class TTRTest extends HazelcastTest {

    // properties
    public String mapBaseName = "map";
    public int mapCount = 10;
    public int batchSize = 1000;
    public long keyDomain = 10000;
    public int valueCount = 100;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int pingCount = 100;

    private String[] values;
    private IMap<Object, Object> pingMap;


    @Setup
    public void setUp() {
        values = generateAsciiStrings(valueCount, minValueLength, maxValueLength);
        pingMap = targetInstance.getMap("pingmap");
    }

    @Prepare(global = true)
    public void prepare() throws ExecutionException, InterruptedException {
        List<CompletableFuture> batch = new ArrayList<>(batchSize);
        Random random = new Random();
        for (long k = 0; k < keyDomain; k++) {
            IMap map = targetInstance.getMap(mapBaseName + k % mapCount);
            String value = values[random.nextInt(valueCount)];
            batch.add(map.putAsync(k, value).toCompletableFuture());

            if (k % 1_000_000 == 0) {
                testContext.echoCoordinator("Insertion at : " + k);
            }

            if (batch.size() == batchSize) {
                waitAndClear(batch);
            }
        }

        waitAndClear(batch);
    }

    private void waitAndClear(List<CompletableFuture> batch) throws ExecutionException, InterruptedException {
        for (Future f : batch) {
            f.get();
        }

        batch.clear();
    }

    @TimeStep
    public void ping(ThreadState state) throws ExecutionException, InterruptedException {
        List<Future> futures = new ArrayList<>(pingCount);
        for (int k = 0; k < pingCount; k++) {
            futures.add(pingMap.getAsync(k).toCompletableFuture());
        }

        for (Future future : futures) {
            future.get();
        }
    }

    public class ThreadState extends BaseThreadState {

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }
}
