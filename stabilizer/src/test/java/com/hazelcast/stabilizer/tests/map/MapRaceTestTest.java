package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.stabilizer.tests.TestFailureException;
import com.hazelcast.stabilizer.tests.TestRunner;
import org.junit.Test;

public class MapRaceTestTest {

    @Test(expected = TestFailureException.class)
    public void test() throws Throwable {
        MapRaceTest test = new MapRaceTest();
        new TestRunner(test).withDuration(10).run();
    }
}
