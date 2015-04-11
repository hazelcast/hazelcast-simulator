package com.hazelcast.simulator.test;

import org.junit.Test;

import static com.hazelcast.simulator.test.TestSuiteTest.createTestSuite;
import static org.junit.Assert.assertNotNull;

public class FailureTest {

    @Test
    public void testConstructor() {
        new Failure();
    }

    @Test
    public void testToString() throws Exception {
        String properties = "FailureTest@class=AtomicLong";
        TestSuite testSuite = createTestSuite(properties);

        Failure failure = new Failure();
        failure.testSuite = testSuite;

        assertNotNull(failure.toString());

        failure.testId = "FailureTest";
        assertNotNull(failure.toString());
    }
}