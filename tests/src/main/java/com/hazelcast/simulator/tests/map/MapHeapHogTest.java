package com.hazelcast.simulator.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.test.utils.TestUtils.humanReadableByteCount;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.nextKeyOwnedBy;

public class MapHeapHogTest {

    private static final ILogger log = Logger.getLogger(MapHeapHogTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 3;
    public int ttlHours = 24;
    public double approxHeapUsageFactor = 0.9;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private long approxEntryBytesSize = 238;

    private IMap map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        map = targetInstance.getMap(basename);
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        if (isMemberNode(targetInstance)) {
            while (targetInstance.getCluster().getMembers().size() != 3) {
                log.info(basename + " waiting cluster == 3");
                Thread.sleep(1000);
            }
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }


    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                long free = Runtime.getRuntime().freeMemory();
                long total = Runtime.getRuntime().totalMemory();
                long used = total - free;
                long max = Runtime.getRuntime().maxMemory();
                long totalFree = max - used;

                long maxLocalEntries = (long) ((totalFree / approxEntryBytesSize) * approxHeapUsageFactor);

                long key = 0;
                for (int i = 0; i < maxLocalEntries; i++) {
                    key = nextKeyOwnedBy(key, targetInstance);
                    map.put(key, key, ttlHours, TimeUnit.HOURS);
                    key++;
                }
                log.info(basename + " after warmUp map size = " + map.size());
                log.info(basename + " putCount = " + maxLocalEntries);
                printMemStats();

                log.info("added All");
            }
        }
    }

    @Verify(global = false)
    public void localVerify() throws Exception {
        if (isMemberNode(targetInstance)) {
            log.info(basename + " verify map size = " + map.size());
            printMemStats();
        }
    }

    public void printMemStats() {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long max = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) max);

        long totalFree = max - used;

        log.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
        log.info(basename + " total free = " + humanReadableByteCount(totalFree, true) + " = " + totalFree);
        log.info(basename + " used = " + humanReadableByteCount(used, true) + " = " + used);
        log.info(basename + " max = " + humanReadableByteCount(max, true) + " = " + max);
        log.info(basename + " usedOfMax = " + usedOfMax + "%");
    }
}
