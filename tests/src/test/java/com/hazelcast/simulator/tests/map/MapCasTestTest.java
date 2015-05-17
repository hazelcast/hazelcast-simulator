package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.test.TestRunner;
import org.junit.After;
import org.junit.Test;

public class MapCasTestTest {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws Exception {
        MapCasTest test = new MapCasTest();
        new TestRunner<MapCasTest>(test).withDuration(10).run();
    }
}
