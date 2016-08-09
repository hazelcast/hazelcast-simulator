package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.test.TestException;
import org.junit.Test;

public class TestPerformanceTrackerTest {

    @Test(expected = TestException.class)
    public void testCreateHistogramLogWriter_withInvalidFilename() {
        TestPerformanceTracker.createHistogramLogWriter("invalidFileName", ":\\//", System.currentTimeMillis());
    }
}
