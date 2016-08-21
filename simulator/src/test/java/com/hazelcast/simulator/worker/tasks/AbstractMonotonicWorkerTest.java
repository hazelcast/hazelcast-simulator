package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.utils.ExceptionReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractMonotonicWorkerTest {

    private static final int THREAD_COUNT = 3;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private enum Operation {
        STOP_TEST_CONTEXT,
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        setupFakeUserDir();
        test = new WorkerTest();
        testContext = new TestContextImpl("AbstractMonotonicWorkerTest");
        TestCase testCase = new TestCase("id").setProperty("threadCount", THREAD_COUNT);
        testContainer = new TestContainer(testContext, test, testCase);

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

    public static class WorkerTest {

        private TestContext testContext;

        private volatile Operation operation;
        private volatile int workerCreated;

        @Setup
        public void setup(TestContext testContext) {
            this.testContext = testContext;
        }

        @RunWithWorker
        public Worker createWorker() {
            workerCreated++;
            return new Worker();
        }

        private class Worker extends AbstractMonotonicWorker {

            @Override
            protected void timeStep() throws Exception {
                switch (operation) {
                    case STOP_TEST_CONTEXT:
                        stopTestContext();
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation: " + operation);
                }
            }
        }
    }
}
