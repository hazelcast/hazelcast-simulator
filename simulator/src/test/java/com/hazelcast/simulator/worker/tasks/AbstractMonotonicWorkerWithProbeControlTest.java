package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import org.HdrHistogram.Histogram;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractMonotonicWorkerWithProbeControlTest {

    private static final int THREAD_COUNT = 3;
    private static final int ITERATION_COUNT = 10;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;
    private File userDir;

    private enum Operation {
        STOP_TEST_CONTEXT,
        USE_MANUAL_PROBE
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        userDir = setupFakeUserDir();

        test = new WorkerTest();
        testContext = new TestContextImpl("AbstractMonotonicWorkerWithProbeControlTest");
        testContainer = new TestContainer(testContext, test,
                new TestCase("foo").setProperty("threadCount", THREAD_COUNT));

        ExceptionReporter.reset();
    }

    @After
    public void tearDown() {
        teardownFakeUserDir();
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
        assertEquals(THREAD_COUNT, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testTimeStep_shouldUseProbe() throws Exception {
        test.operation = Operation.USE_MANUAL_PROBE;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(test.probe);
        Histogram intervalHistogram = ((HdrProbe) test.probe).getIntervalHistogram();
        assertEquals(THREAD_COUNT * ITERATION_COUNT, intervalHistogram.getTotalCount());
        assertEquals(THREAD_COUNT, test.workerCreated);
    }

    public static class WorkerTest {

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
