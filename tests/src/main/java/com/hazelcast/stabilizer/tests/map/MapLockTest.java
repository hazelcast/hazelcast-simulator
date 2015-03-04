package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.RunWithWorker;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.stabilizer.worker.tasks.AbstractWorker;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link IMap#lock(Object)} method.
 * <p/>
 * We use {@link IMap#lock(Object)} to control concurrent access to map key/value pairs. There are a total of {@link #keyCount}
 * keys stored in a map which are initialized to zero, we concurrently increment the value of a random key. We keep track of all
 * increments to each key and verify the value in the map for each key is equal to the total increments done on each key.
 */
public class MapLockTest {

    private final static ILogger log = Logger.getLogger(MapLockTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 3;
    public int keyCount = 1000;

    private IMap<Integer, Long> map;
    private IList<long[]> incrementsList;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        map = targetInstance.getMap(basename);
        incrementsList = targetInstance.getList(basename);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0l);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        long[] expected = new long[keyCount];

        for (long[] increments : incrementsList) {
            for (int i = 0; i < keyCount; i++) {
                expected[i] += increments[i];
            }
        }
        log.info(format("%s: collected increments from %d worker threads", basename, incrementsList.size()));

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (expected[i] != map.get(i)) {
                failures++;
            }
        }
        assertEquals(format("%s: %d keys have been incremented unexpectedly out of %d keys", basename, failures, keyCount), 0,
                failures);
    }

    @RunWithWorker
    public AbstractWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private final long[] increments = new long[keyCount];

        @Override
        public void timeStep() {
            int key = randomInt(keyCount);
            long increment = randomInt(100);

            map.lock(key);
            try {
                Long current = map.get(key);
                map.put(key, current + increment);
                increments[key] += increment;
            } finally {
                map.unlock(key);
            }
        }

        @Override
        protected void afterRun() {
            incrementsList.add(increments);
        }
    }
}
