package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.Random;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrowingMapTest {
    private static final ILogger log = Logger.getLogger(GrowingMapTest.class);

    // properties
    public int threadCount = 10;
    public int growCount = 10000;
    public boolean usePut = true;
    public boolean useRemove = true;
    public int logFrequency = 10000;
    public boolean removeOnStop = true;
    public boolean readValidation = true;
    public String basename = "growingMap";

    private TestContext testContext;
    private IdGenerator idGenerator;
    private IMap<Long, Long> map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;

        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        idGenerator = hazelcastInstance.getIdGenerator(testContext.getTestId() + ":IdGenerator");
        map = hazelcastInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    @Verify
    public void verify() throws Exception {
        if (removeOnStop) {
            assertEquals("Map should be empty, but has size:", 0, map.size());
            assertTrue("Map should be empty, but has size:", map.isEmpty());
        }
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        private final long[] keys = new long[growCount];
        private final long[] values = new long[growCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int keyIndex = insert();
                if (readValidation) {
                    read(keyIndex);
                }
                delete(keyIndex);
            }
        }

        private int insert() {
            int insertIteration = 0;
            while (!testContext.isStopped() && insertIteration < growCount) {
                long key = idGenerator.newId();
                long value = random.nextLong();

                keys[insertIteration] = key;
                values[insertIteration] = value;

                if (usePut) {
                    map.put(key, value);
                } else {
                    map.set(key, value);
                }

                insertIteration++;
                if (insertIteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At insert iteration: " + insertIteration);
                }
            }

            log.info(Thread.currentThread().getName() + " Inserted " + insertIteration + " key/value pairs");
            return insertIteration;
        }

        private void read(int keyIndex) {
            int readIteration = 0;
            while (!testContext.isStopped() && readIteration < keyIndex) {
                long key = keys[readIteration];
                long value = values[readIteration];

                long found = map.get(key);
                if (found != value) {
                    throw new RuntimeException("Unexpected value found");
                }

                readIteration++;
                if (readIteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At read iteration: " + readIteration);
                }
            }
        }

        private void delete(int keyIndex) {
            int deleteIteration = 0;
            while ((!testContext.isStopped() || removeOnStop) && deleteIteration < keyIndex) {
                long key = keys[deleteIteration];
                long value = values[deleteIteration];

                if (useRemove) {
                    long found = map.remove(key);
                    if (found != value) {
                        throw new RuntimeException(format("Expected: %d, but was %d", value, found));
                    }
                } else {
                    map.delete(key);
                }

                deleteIteration++;
                if (deleteIteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At delete iteration: " + deleteIteration);
                }
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        GrowingMapTest test = new GrowingMapTest();
        new TestRunner<GrowingMapTest>(test).withDuration(30).run();
    }
}
