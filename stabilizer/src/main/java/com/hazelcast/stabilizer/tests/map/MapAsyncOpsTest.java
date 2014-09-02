package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.map.helpers.MapOperationsCount;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;


public class MapAsyncOpsTest {
    private final static ILogger log = Logger.getLogger(MapAsyncOpsTest.class);

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 10;

    //check these add up to 1
    public double PutAsyncProb = 0.2;
    public double PutAsyncTTLProb = 0.2;
    public double getAsyncProb = 0.2;
    public double removeAsyncProb = 0.2;
    public double destroyProb = 0.2;
    //
    public int maxTTLExpireySeconds = 3;


    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private MapOperationsCount count = new MapOperationsCount();

    public MapAsyncOpsTest() {
    }

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();

        IList results = targetInstance.getList(basename + "report");
        results.add(count);
    }


    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    final int key = random.nextInt(keyCount);
                    final IMap map = targetInstance.getMap(basename);

                    double chance = random.nextDouble();
                    if ((chance -= PutAsyncProb) < 0) {
                        final Object value = random.nextInt();
                        map.putAsync(key, value);
                        count.putAsyncCount.incrementAndGet();
                    }
                    if ((chance -= PutAsyncTTLProb) < 0) {
                        final Object value = random.nextInt();
                        int delay = 1 + random.nextInt(maxTTLExpireySeconds);
                        map.putAsync(key, value, delay, TimeUnit.SECONDS);
                        count.putAsyncTTLCount.incrementAndGet();
                    } else if ((chance -= getAsyncProb) < 0) {
                        map.getAsync(key);
                        count.getAsyncCount.incrementAndGet();
                    } else if ((chance -= removeAsyncProb) < 0) {
                        map.removeAsync(key);
                        count.removeAsyncCount.incrementAndGet();
                    } else if ((chance -= destroyProb) <= 0) {
                        map.destroy();
                        count.destroyCount.incrementAndGet();
                    }

                } catch (DistributedObjectDestroyedException e) {
                }
            }
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<MapOperationsCount> results = targetInstance.getList(basename + "report");
        MapOperationsCount total = new MapOperationsCount();
        for (MapOperationsCount i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " total of " + results.size());
    }

    @Verify(global = false)
    public void verify() throws Exception {
        try {
            Thread.sleep(maxTTLExpireySeconds * 2);

            final IMap map = targetInstance.getMap(basename);

            log.info(basename + ": map size  =" + map.size());

        } catch (UnsupportedOperationException e) {
            // TODO: Why is this exception caught??
        }
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new MapAsyncOpsTest()).run();
    }
}