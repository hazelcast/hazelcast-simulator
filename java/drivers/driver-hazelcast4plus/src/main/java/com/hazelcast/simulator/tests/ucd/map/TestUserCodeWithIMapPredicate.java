package com.hazelcast.simulator.tests.ucd.map;

import com.hazelcast.collection.IList;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.ucd.UCDTest;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class TestUserCodeWithIMapPredicate extends UCDTest {

    // properties
    public int keyCount = 1;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private IMap<Integer, Integer> map;
    private IList<Long> resultsPerWorker;
    private final AtomicLong localReturns = new AtomicLong(0);

    @Override
    @Setup
    public void setUp() throws ReflectiveOperationException {
        super.setUp();
        map = targetInstance.getMap(name);
        resultsPerWorker = targetInstance.getList(name + ":ResultMap");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, i);
        }
    }

    @TimeStep
    public void timeStep() throws ReflectiveOperationException {
        int returnSize = map.values(getUDFInstance()).size();
        localReturns.addAndGet(returnSize);
    }

    @Verify
    public void verify() {
        int failures = 0;
        for (Long result : resultsPerWorker) {
            if (result % 500 != 0) {
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
}
