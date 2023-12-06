package com.hazelcast.simulator.tests.ucd;

import com.hazelcast.collection.IList;
import com.hazelcast.config.NamespaceConfig;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import java.net.URL;
import java.net.URLClassLoader;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

public class TestUCDWithIMapEntryProcessor extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private IMap<Integer, Long> map;
    private IList<long[]> resultsPerWorker;
    private Class clazz;
    private int[] keys;

    @Setup
    public void setUp() throws Exception {
        if (minProcessorDelayMs > maxProcessorDelayMs) {
            throw new IllegalArgumentException("minProcessorDelayMs has to be >= maxProcessorDelayMs. "
                    + "Current settings: minProcessorDelayMs = " + minProcessorDelayMs
                    + " maxProcessorDelayMs = " + maxProcessorDelayMs);
        }

        loadEntryProcessor();
        configureNamespace();
        map = targetInstance.getMap(name);
        resultsPerWorker = targetInstance.getList(name + ":ResultMap");
        keys = KeyUtils.generateIntKeys(keyCount, keyLocality, targetInstance);
    }

    private void loadEntryProcessor() throws ClassNotFoundException {
        URL url = getClass().getClassLoader().getResource("class/com/hazelcast/simulator/tests/ucd/");
        URL[] urls = new URL[]{url};
        // Create a new class loader with the directory
        ClassLoader cl = new URLClassLoader(urls);
        clazz = cl.loadClass("IncrementEntryProcessor");
    }

    private void configureNamespace() {
        NamespaceConfig ns = new NamespaceConfig();
        ns.setName("ns1");
        ns.addClass(clazz);

        targetInstance.getConfig().getNamespacesConfig().addNamespaceConfig(ns);
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(TestUCDWithIMapEntryProcessor.ThreadState state) throws Exception {
        int key = keys[state.randomInt(keys.length)];

        long increment = state.randomInt(100);
        int delayMs = state.calculateDelay();

        map.executeOnKey(key, (EntryProcessor<Integer, Long, Object>)
                clazz.getDeclaredConstructor(long.class, int.class).newInstance(increment, delayMs));

        state.localIncrementsAtKey[key] += increment;
    }

    @AfterRun
    public void afterRun(TestUCDWithIMapEntryProcessor.ThreadState state) {
        // sleep to give time for the last EntryProcessor tasks to complete
        sleepMillis(maxProcessorDelayMs * 2);
        resultsPerWorker.add(state.localIncrementsAtKey);
    }

    public class ThreadState extends BaseThreadState {

        private final long[] localIncrementsAtKey = new long[keyCount];

        private int calculateDelay() {
            int delayMs = 0;
            if (minProcessorDelayMs >= 0 && maxProcessorDelayMs > 0) {
                delayMs = minProcessorDelayMs + randomInt(1 + maxProcessorDelayMs - minProcessorDelayMs);
            }
            return delayMs;
        }
    }

    @Verify
    public void verify() {
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

    @Teardown
    public void tearDown() {
        map.destroy();
        resultsPerWorker.destroy();
    }

    /**
     * TESTS
     * 1 (on my list) / UCD - 5.3.6, using the same EntryProcessor (accessed via reflection)  - classpath.
     * 1 (global) / UCD - 5.4.0, using the same EntryProcessor (accessed via reflection) - classpath.
     * 2 - number 1, but 5.4.0 (namespaces) w/ namespaces enabled - but no actual namespaces configured. - classpath.
     * 3 - number 2, but with namespaces configured and the class not on the classpath.
     */
}
