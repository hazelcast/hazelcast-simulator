package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestContextImpl;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import org.HdrHistogram.Histogram;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractMonotonicWorkerWithProbeControlTest {

    private static final int THREAD_COUNT = 3;
    private static final int ITERATION_COUNT = 10;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private enum Operation {
        STOP_TEST_CONTEXT,
        CALL_TIMESTEP_WITH_ENUM,
        USE_MANUAL_PROBE
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        test = new WorkerTest();
        testContext = new TestContextImpl("AbstractMonotonicWorkerWithProbeControlTest");
        testContainer = new TestContainer(testContext, test, THREAD_COUNT);

        ExceptionReporter.reset();
    }

    @After
    public void tearDown() {
        for (int i = 1; i <= THREAD_COUNT; i++) {
            deleteQuiet(i + ".exception");
        }

        ExceptionReporter.reset();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testInvokeSetup() throws Exception {
        testContainer.invoke(TestPhase.SETUP);

        assertEquals(testContext, test.testContext);
        assertEquals(0, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRun() throws Exception {
        test.operation = Operation.STOP_TEST_CONTEXT;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.testContext.isStopped());
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testTimeStep_withOperation_shouldThrowException() throws Exception {
        test.operation = Operation.CALL_TIMESTEP_WITH_ENUM;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        for (int i = 1; i <= THREAD_COUNT; i++) {
            assertTrue(new File(i + ".exception").exists());
        }
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testTimeStep_shouldUseProbe() throws Exception {
        test.operation = Operation.USE_MANUAL_PROBE;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(test.probe);
        Histogram intervalHistogram = test.probe.getIntervalHistogram();
        assertEquals(THREAD_COUNT * ITERATION_COUNT, intervalHistogram.getTotalCount());
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    private static class WorkerTest {

        private TestContext testContext;

        private volatile Operation operation;
        private volatile int workerCreated;
        private volatile Probe probe;

        @Setup
        public void setup(TestContext testContext) {
            this.testContext = testContext;
        }

        @RunWithWorker
        public Worker createWorker() {
            workerCreated++;
            return new Worker(this);
        }

        private class Worker extends AbstractMonotonicWorkerWithProbeControl {

            private final WorkerTest test;

            Worker(WorkerTest test) {
                this.test = test;
            }

            @Override
            protected void timeStep(Probe probe) throws Exception {
                switch (operation) {
                    case STOP_TEST_CONTEXT:
                        stopTestContext();
                        break;
                    case CALL_TIMESTEP_WITH_ENUM:
                        timeStep(operation);
                        break;
                    case USE_MANUAL_PROBE:
                        long started = System.nanoTime();
                        if (getIteration() == ITERATION_COUNT) {
                            test.probe = probe;
                            stopWorker();
                            break;
                        }
                        probe.recordValue(System.nanoTime() - started);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation: " + operation);
                }
            }
        }
    }
}
