package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.test.TestException;
import org.junit.Test;

public class PerformanceTrackerTest {

    @Test(expected = TestException.class)
    public void testCreateHistogramLogWriter_withInvalidFilename() {
        PerformanceTracker.createHistogramLogWriter("invalidFileName", ":\\//", System.currentTimeMillis());
    }

    @Test(expected = TestException.class)
    public void testCreateHistogramLogReader_withInvalidFilename() {
        PerformanceTracker.createHistogramLogReader("invalidFileName", ":\\//");
    }
}
