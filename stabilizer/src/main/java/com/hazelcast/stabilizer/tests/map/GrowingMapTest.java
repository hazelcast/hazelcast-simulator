package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrowingMapTest {

    private final static ILogger log = Logger.getLogger(GrowingMapTest.class);

    //props.
    public int threadCount = 10;
    public int growCount = 10000;
    public boolean usePut = true;
    public boolean useRemove = true;
    public int logFrequency = 10000;
    public boolean removeOnStop = true;
    public boolean readValidation = true;
    public String basename = "growningmap";

    private IMap<Long, Long> map;
    private IdGenerator idGenerator;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;

        targetInstance = testContext.getTargetInstance();
        idGenerator = targetInstance.getIdGenerator(testContext.getTestId() + ":IdGenerator");
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Teardown
    public void teardown() throws Exception {
        //map.destroy();
    }

    @Verify
    public void verify() throws Exception {

        if(removeOnStop){
            assertEquals("Map should be empty, but has size:", 0, map.size());
            assertTrue("Map should be empty, but has size:", map.isEmpty());
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            long insertIteration = 0;
            long deleteIteration = 0;
            long readIteration = 0;

            long[] keys = new long[growCount];
            long[] values = new long[growCount];

            Random random = new Random();

            while (!testContext.isStopped()) {
                int keyIndex = -1;
                for (int k = 0; k < growCount; k++) {
                    if (testContext.isStopped()) {
                        break;
                    }

                    long key = idGenerator.newId();
                    long value = random.nextLong();
                    keyIndex = k;
                    keys[keyIndex] = key;
                    values[keyIndex] = value;

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

                if (readValidation) {
                    for (int k = 0; k <= keyIndex; k++) {
                        if (testContext.isStopped()) {
                            break;
                        }

                        long key = keys[k];
                        long value = values[k];

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

                for (int k = 0; k <= keyIndex; k++) {
                    if (testContext.isStopped() && !removeOnStop) {
                        break;
                    }

                    long key = keys[k];
                    long value = values[k];

                    if (useRemove) {
                        long found = map.remove(key);
                        if (found != value) {
                            throw new RuntimeException("Unexpected value found");
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
    }

    public static void main(String[] args) throws Throwable {
        GrowingMapTest test = new GrowingMapTest();
        new TestRunner(test).run();
    }
}
