package com.hazelcast.simulator.tests.special;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;

/**
 * This test is to debug and check probe results from a very controlled test case.
 *
 * By adjusting the threadCount and maxOperations the invocation count of probes are absolutely predictable.
 */
public class ProbeConcurrencyTest {

    private static final ILogger LOGGER = Logger.getLogger(ProbeConcurrencyTest.class);

    // properties
    public String basename = ProbeConcurrencyTest.class.getSimpleName();
    public int threadCount = 0;
    public int maxOperations = 0;

    @Setup
    public void setUp(TestContext testContext) {
        LOGGER.info("ThreadCount: " + threadCount + " max operations: " + maxOperations);
    }

    @RunWithWorker
    public IWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private int operationCount;

        @Override
        protected void timeStep() throws Exception {
            if (++operationCount >= maxOperations) {
                stopWorker();
            }
        }
    }
}
