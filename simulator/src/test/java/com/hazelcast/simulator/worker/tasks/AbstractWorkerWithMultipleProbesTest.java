package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestContextImpl;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import org.HdrHistogram.Histogram;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteExceptionLogs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AbstractWorkerWithMultipleProbesTest {

    private static final int THREAD_COUNT = 3;
    private static final int ITERATION_COUNT = 10;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private enum Operation {
        EXCEPTION,
        STOP_WORKER,
        STOP_TEST_CONTEXT,
        RANDOM,
        ITERATION
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        test = new WorkerTest();
        testContext = new TestContextImpl("AbstractWorkerWithMultipleProbesTest");
        testContainer = new TestContainer(testContext, test,
                new TestCase("foo").setProperty("threadCount",THREAD_COUNT));

        ExceptionReporter.reset();
    }

    @After
    public void tearDown() {
        deleteExceptionLogs(THREAD_COUNT);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testInvokeSetup() throws Exception {
        testContainer.invoke(TestPhase.SETUP);

        assertEquals(testContext, test.testContext);
        assertEquals(0, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRun_withException() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.EXCEPTION);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        for (int i = 1; i <= THREAD_COUNT; i++) {
            assertTrue(new File(i + ".exception").exists());
        }
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testStopWorker() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.STOP_WORKER);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertFalse(test.testContext.isStopped());
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testStopTestContext() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.STOP_TEST_CONTEXT);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.testContext.isStopped());
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRandomMethods() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.RANDOM);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(test.randomInt);
        assertNotNull(test.randomIntWithBond);
        assertNotNull(test.randomLong);
    }

    @Ignore
    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testGetIteration() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.ITERATION);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertEquals(ITERATION_COUNT, test.testIteration);
        assertNotNull(test.probe);
        Histogram intervalHistogram = ((HdrProbe)test.probe).getIntervalHistogram();
        assertEquals(THREAD_COUNT * ITERATION_COUNT, intervalHistogram.getTotalCount());
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
        assertEquals(1, testContainer.getProbeMap().size());
    }

    private static class WorkerTest {

        private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

        private TestContext testContext;

        private volatile int workerCreated;
        private volatile Integer randomInt;
        private volatile Integer randomIntWithBond;
        private volatile Long randomLong;
        private volatile long testIteration;
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

        private class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

            private final AbstractWorkerWithMultipleProbesTest.WorkerTest test;

            Worker(AbstractWorkerWithMultipleProbesTest.WorkerTest test) {
                super(operationSelectorBuilder);
                this.test = test;
            }

            @Override
            protected void timeStep(Operation operation, Probe probe) throws Exception {
                switch (operation) {
                    case EXCEPTION:
                        throw new TestException("expected exception");
                    case STOP_WORKER:
                        stopWorker();
                        break;
                    case STOP_TEST_CONTEXT:
                        stopTestContext();
                        break;
                    case RANDOM:
                        randomInt = randomInt();
                        randomIntWithBond = randomInt(1000);
                        randomLong = getRandom().nextLong();
                        stopTestContext();
                        break;
                    case ITERATION:
                        long started = System.nanoTime();
                        if (getIteration() == ITERATION_COUNT) {
                            test.probe = probe;
                            testIteration = getIteration();
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
