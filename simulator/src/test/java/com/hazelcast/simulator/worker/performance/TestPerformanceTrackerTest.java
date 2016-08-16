package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.test.TestException;
import org.junit.Test;

import static com.hazelcast.simulator.worker.performance.TestPerformanceTracker.createHistogramLogWriter;

public class TestPerformanceTrackerTest {

    @Test(expected = TestException.class)
    public void testCreateHistogramLogWriter_withInvalidFilename() {
        createHistogramLogWriter("invalidFileName", ":\\//", System.currentTimeMillis());
    }
}
