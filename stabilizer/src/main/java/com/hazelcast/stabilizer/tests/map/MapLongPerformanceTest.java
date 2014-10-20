package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Name;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class MapLongPerformanceTest {

    private final static ILogger log = Logger.getLogger(MapLongPerformanceTest.class);

    //props
    public int threadCount = 10;
    public int keyCount = 1000000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = "maplong";
    public int writePercentage = 10;

    private IMap<Integer, Long> map;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private SimpleProbe setProbe;
    private SimpleProbe getProbe;
    private IntervalProbe intervalProbe;

    @Setup
    public void setup(TestContext testContext,
                      @Name("set") SimpleProbe setProbe,
                      @Name("get") SimpleProbe getProbe,
                      @Name("latencyProbe")IntervalProbe intervalProbe) throws Exception {
        this.setProbe = setProbe;
        this.getProbe = getProbe;
        this.intervalProbe = intervalProbe;
        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

        this.testContext = testContext;

        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
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

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        @Override
        public void run() {
            for (int k = 0; k < keyCount; k++) {
                result.put(k, 0L);
            }

            long iteration = 0;
            while (!testContext.isStopped()) {
                Integer key = random.nextInt(keyCount);
                if (shouldWrite(iteration)) {
                    intervalProbe.started();
                    try {
                        map.set(key, System.currentTimeMillis());
                    } finally {
                        intervalProbe.done();
                    }
                    setProbe.done();
                } else {
                    intervalProbe.started();
                    try {
                        map.get(key);
                    } finally {
                        intervalProbe.done();
                    }
                    getProbe.done();
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }
        }

        private boolean shouldWrite(long iteration) {
            if (writePercentage == 0) {
                return false;
            } else if (writePercentage == 100) {
                return true;
            } else {
                return (iteration % 100) < writePercentage;
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        MapLongPerformanceTest test = new MapLongPerformanceTest();
        new TestRunner(test).run();
    }
}
