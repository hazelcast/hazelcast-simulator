package com.hazelcast.simulator.tests.map;

import com.hazelcast.simulator.test.TestRunner;
import org.junit.Test;

public class MapRaceTestTest {

    @Test(expected = AssertionError.class)
    public void test() throws Throwable {
        MapRaceTest test = new MapRaceTest();
        new TestRunner<MapRaceTest>(test).withDuration(10).run();
    }
}
