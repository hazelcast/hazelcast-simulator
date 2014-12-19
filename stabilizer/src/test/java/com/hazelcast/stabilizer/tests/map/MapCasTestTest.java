package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.stabilizer.tests.TestRunner;
import org.junit.Test;

public class MapCasTestTest {
    @Test
    public void test() throws Throwable {
        MapCasTest test = new MapCasTest();
        new TestRunner<MapCasTest>(test).withDuration(10).run();
    }
}
