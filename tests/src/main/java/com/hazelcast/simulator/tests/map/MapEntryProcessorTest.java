package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKey;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

// FIXME get rid of this suppression via a proper @InjectProbe annotation
@SuppressFBWarnings({"UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD", "NP_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD"})
public class MapEntryProcessorTest {

    // properties
    public String basename = MapEntryProcessorTest.class.getSimpleName();
    public int keyCount = 1000;
    public int minProcessorDelayMs;
    public int maxProcessorDelayMs;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public Probe probe;

    private HazelcastInstance targetInstance;
    private IMap<Integer, Long> map;
    private IList<long[]> resultsPerWorker;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        if (minProcessorDelayMs > maxProcessorDelayMs) {
            throw new IllegalArgumentException("minProcessorDelayMs has to be >= maxProcessorDelayMs. "
                    + "Current settings: minProcessorDelayMs = " + minProcessorDelayMs
                    + " maxProcessorDelayMs = " + maxProcessorDelayMs);
        }

        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
        resultsPerWorker = targetInstance.getList(basename + "ResultMap" + testContext.getTestId());
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] expectedValueForKey = new long[keyCount];

        for (long[] incrementsAtKey : resultsPerWorker) {
            for (int i = 0; i < incrementsAtKey.length; i++) {
                expectedValueForKey[i] += incrementsAtKey[i];
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long expected = expectedValueForKey[i];
            long found = map.get(i);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals(0, failures);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private final long[] localIncrementsAtKey = new long[keyCount];

        @Override
        public void timeStep() {
            int key = generateIntKey(keyCount, keyLocality, targetInstance);
            long increment = randomInt(100);
            int delayMs = calculateDelay();
            probe.started();
            map.executeOnKey(key, new IncrementEntryProcessor(increment, delayMs));
            probe.done();
            localIncrementsAtKey[key] += increment;
        }

        @Override
        protected void afterRun() {
            // sleep to give time for the last EntryProcessor tasks to complete
            sleepMillis(maxProcessorDelayMs * 2);
            resultsPerWorker.add(localIncrementsAtKey);
        }

        private int calculateDelay() {
            int delayMs = 0;
            if (minProcessorDelayMs >= 0 && maxProcessorDelayMs > 0) {
                delayMs = minProcessorDelayMs + randomInt(1 + maxProcessorDelayMs - minProcessorDelayMs);
            }
            return delayMs;
        }
    }

    private static final class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Long> {
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

    public static void main(String[] args) throws Exception {
        MapEntryProcessorTest test = new MapEntryProcessorTest();
        new TestRunner<MapEntryProcessorTest>(test).run();
    }
}
