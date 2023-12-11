package com.hazelcast.simulator.tests.ucd.map;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.ucd.UCDTest;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestUserCodeWithIMapEntryProcessor extends UCDTest {
    private IMap<Integer, Long> map;
    private final int key = 0;
    private final AtomicLong expectedIncrement = new AtomicLong(0);

    @Override
    @Setup
    public void setUp() throws ReflectiveOperationException {
        super.setUp();
        map = targetInstance.getMap(name);
        //map lazily created, push entry to force creation.
        map.set(key, 0L);
    }

    @TimeStep
    public void timeStep() throws ReflectiveOperationException  {
        map.executeOnKey(key, (EntryProcessor<Integer, Long, Object>)
                udf.getDeclaredConstructor(long.class).newInstance(key));

        expectedIncrement.incrementAndGet();
    }

    @Verify
    public void verify() {
        assertEquals((long) map.get(key), expectedIncrement.get());
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    /**
     * TESTS
     * 1 (on my list) / UCD - 5.3.6, using the same EntryProcessor (accessed via reflection)  - classpath.
     * 1 (global) / UCD - 5.4.0, using the same EntryProcessor (accessed via reflection) - classpath.
     * 2 - number 1, but 5.4.0 (namespaces) w/ namespaces enabled - but no actual namespaces configured. - classpath.
     * 3 - number 2, but with namespaces configured and the class not on the classpath.
     */
}
