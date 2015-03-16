package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

public class MapEntryProcessorTest {

    private static final ILogger log = Logger.getLogger(MapEntryProcessorTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;
    public KeyLocality keyLocality = KeyLocality.Random;
    public IntervalProbe probe;

    private HazelcastInstance targetInstance;
    private IMap<Integer, Long> map;
    private IList<Map<Integer, Long>> resultsPerWorker;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        if (minProcessorDelayMs > maxProcessorDelayMs) {
            throw new IllegalArgumentException("minProcessorDelayMs has to be >= maxProcessorDelayMs. " +
                    "Current settings: minProcessorDelayMs = " + minProcessorDelayMs +
                    " maxProcessorDelayMs = " + maxProcessorDelayMs);
        }

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
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0l);
        }
        log.info(basename + " map size ==>" + map.size());
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

    @RunWithWorker
    public AbstractWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        @Override
        protected void beforeRun() {
            for (int i = 0; i < keyCount; i++) {
                result.put(i, 0L);
            }
        }

        @Override
        public void timeStep() {
            int key = calculateKey();
            long increment = calculateIncrement();
            int delayMs = calculateDelay();
            probe.started();
            map.executeOnKey(key, new IncrementEntryProcessor(increment, delayMs));
            probe.done();
            incrementLocalStats(key, increment);
        }

        @Override
        public void afterCompletion() {
            // sleep to give time for the last EntryProcessor tasks to complete
            sleepMillis(maxProcessorDelayMs * 2);
            resultsPerWorker.add(result);
        }

        private int calculateKey() {
            return KeyUtils.generateIntKey(keyCount, keyLocality, targetInstance);
        }

        private int calculateIncrement() {
            return randomInt(100);
        }

        private int calculateDelay() {
            int delayMs = 0;
            if (minProcessorDelayMs >= 0 && maxProcessorDelayMs > 0) {
                delayMs = minProcessorDelayMs + randomInt(1 + maxProcessorDelayMs - minProcessorDelayMs);
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

