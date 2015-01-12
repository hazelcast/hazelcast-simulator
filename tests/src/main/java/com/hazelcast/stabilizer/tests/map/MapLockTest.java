package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class MapLockTest {

    private final static ILogger log = Logger.getLogger(MapLockTest.class);

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 1000;

    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IMap map = targetInstance.getMap(basename);
        for (int k = 0; k < keyCount; k++) {
            map.put(k, 0l);
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
        private final Random random = new Random();
        private long[] increments = new long[keyCount];

        public void run() {
            while (!testContext.isStopped()) {

                IMap<Integer, Long> map = targetInstance.getMap(basename);
                int key = random.nextInt(keyCount);
                map.lock(key);
                try {
                    long current = map.get(key);
                    long increment = random.nextInt(100);

                    map.put(key, current + increment);
                    increments[key]+=increment;
                } finally {
                    map.unlock(key);
                }
            }
            targetInstance.getList(basename).add(increments);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        long[] total = new long[keyCount];

        IList<long[]> allIncrements = targetInstance.getList(basename);
        for (long[] increments : allIncrements) {
            for(int i=0; i<keyCount; i++){
                total[i]+=increments[i];
            }
        }
        log.info(basename + ": collected increments from " + allIncrements.size() + " worker threads");

        IMap<Integer, Long> map = targetInstance.getMap(basename);
        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (total[i] != map.get(i)) {
                failures++;
            }
        }
        assertEquals(basename + ": " + failures + " keys have been incremented unexpectedly out of " + keyCount + " keys", 0, failures);
    }
}