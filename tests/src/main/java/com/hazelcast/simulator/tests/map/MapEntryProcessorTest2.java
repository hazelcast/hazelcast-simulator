package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.*;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.*;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static junit.framework.TestCase.assertEquals;

public class MapEntryProcessorTest2 {

    private final static ILogger log = Logger.getLogger(MapEntryProcessorTest2.class);

    public String basename = this.getClass().getName();
    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;

    private IMap<Integer, Long> map;
    private IList<long[]> allIncrementsOnKeys;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        if (minProcessorDelayMs > maxProcessorDelayMs) {
            throw new IllegalArgumentException("minProcessorDelayMs has to be >= maxProcessorDelayMs. " +
                    "Current settings: minProcessorDelayMs = "+minProcessorDelayMs +
                    " maxProcessorDelayMs = "+maxProcessorDelayMs);
        }

        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        allIncrementsOnKeys = targetInstance.getList(basename + "Result");
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        allIncrementsOnKeys.destroy();
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

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final long[] localIncrementsAtKey = new long[keyCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int delayMs = calculateDelay();
                int key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                map.executeOnKey(key, new IncrementEntryProcessor(increment, delayMs));
                localIncrementsAtKey[key] += increment;
            }

            //sleep to give time for the last EntryProcessor tasks to complete.
            sleepMillis(maxProcessorDelayMs * 2);
            allIncrementsOnKeys.add(localIncrementsAtKey);
        }

        private int calculateDelay() {
            int delayMs = 0;
            if (maxProcessorDelayMs != 0) {
                delayMs = minProcessorDelayMs + random.nextInt(maxProcessorDelayMs - minProcessorDelayMs + 1);
            }
            return delayMs;
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] expectedValueForKey = new long[keyCount];

        for (long[] incrementsAtKey : allIncrementsOnKeys) {
            for (int k=0; k<incrementsAtKey.length; k++) {
                expectedValueForKey[k] += incrementsAtKey[k];
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long actual = map.get(k);
            assertEquals(basename + ": expectedValueForKey " + k + " not in the map at key " + k, expectedValueForKey[k], actual);
        }

        log.info(basename + " OKOOKOKKKKKKKK");
    }

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Long> {
        private final long increment;
        private final long delayMs;

        private IncrementEntryProcessor(long increment, long delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(Map.Entry<Integer, Long> entry) {
            sleepMillis((int)delayMs);
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }
    }

    public static void main(String[] args) throws Throwable {
        MapEntryProcessorTest2 test = new MapEntryProcessorTest2();
        new TestRunner<MapEntryProcessorTest2>(test).run();
    }
}

