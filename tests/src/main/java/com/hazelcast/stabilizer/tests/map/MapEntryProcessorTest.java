package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

public class MapEntryProcessorTest {

    private final static ILogger log = Logger.getLogger(MapEntryProcessorTest.class);

    //props
    public String basename = this.getClass().getName();
    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;
    public KeyLocality keyLocality = KeyLocality.Random;

    private IMap<Integer, Long> map;
    private IList<Map<Integer, Long>> resultsPerWorker;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    private IntervalProbe latency;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        if (minProcessorDelayMs > maxProcessorDelayMs) {
            throw new IllegalArgumentException("minProcessorDelayMs has to be >= maxProcessorDelayMs. " +
                    "Current settings: minProcessorDelayMs = "+minProcessorDelayMs +
                    " maxProcessorDelayMs = "+maxProcessorDelayMs);
        }

        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
        resultsPerWorker = targetInstance.getList(basename + "ResultMap" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < keyCount; k++) {
            map.put(k, 0l);
        }
        log.info(basename + " map size ==>" + map.size());
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long expected = amount[k];
            long found = map.get(k);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals(0, failures);
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        public Worker() {
            for (int k = 0; k < keyCount; k++) {
                result.put(k, 0L);
            }
        }

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                long increment = calculateIncrement();
                int delayMs = calculateDelay();
                int key = calculateKey();
                latency.started();
                map.executeOnKey(key, new IncrementEntryProcessor(increment, delayMs));
                latency.done();
                incrementLocalStats(key, increment);
            }

            // sleep to give time for the last EntryProcessor tasks to complete.
            sleepMillis(maxProcessorDelayMs * 2);
            resultsPerWorker.add(result);
        }

        private int calculateKey() {
            return KeyUtils.generateIntKey(keyCount, keyLocality, targetInstance);
        }

        private int calculateIncrement() {
            return random.nextInt(100);
        }

        private int calculateDelay() {
            int delayMs = 0;
            if (maxProcessorDelayMs != 0) {
                delayMs = minProcessorDelayMs + random.nextInt(maxProcessorDelayMs - minProcessorDelayMs + 1);
            }
            return delayMs;
        }

        private void incrementLocalStats(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Long> {
        private final long increment;
        private final int delayMs;

        private IncrementEntryProcessor(long increment, int delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(Map.Entry<Integer, Long> entry) {
            sleepMillis(delayMs);
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }
    }

    public static void main(String[] args) throws Throwable {
        MapEntryProcessorTest test = new MapEntryProcessorTest();
        new TestRunner<MapEntryProcessorTest>(test).run();
    }
}

