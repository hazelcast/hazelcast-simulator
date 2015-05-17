package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.test.TestRunner;
import org.junit.After;
import org.junit.Test;

public class MapRaceTestTest {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = AssertionError.class)
    public void test() throws Exception {
        MapRaceTest test = new MapRaceTest();
        new TestRunner<MapRaceTest>(test).withDuration(10).run();
    }
}
