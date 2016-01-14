package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestContextImpl;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractMonotonicWorkerTest {

    private static final int THREAD_COUNT = 3;

    private enum Operation {
        STOP_TEST_CONTEXT,
        CALL_TIMESTEP_WITH_ENUM
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("logFrequency", "5");
        properties.put("threadCount", String.valueOf(THREAD_COUNT));

        test = new WorkerTest();
        testContext = new TestContextImpl("AbstractMonotonicWorkerTest", null);
        TestCase testCase = new TestCase("AbstractMonotonicWorkerTest", properties);
        testContainer = new TestContainer(test, testContext, testCase);

        ExceptionReporter.reset();
    }

    @After
    public void tearDown() throws Exception {
        for (int i = 1; i <= THREAD_COUNT; i++) {
            deleteQuiet(new File(i + ".exception"));
        }

        ExceptionReporter.reset();
    }

    @Test
    public void testInvokeSetup() throws Exception {
        testContainer.invoke(TestPhase.SETUP);

        assertEquals(testContext, test.testContext);
        assertEquals(0, test.workerCreated);
    }

    @Test
    public void testRun() throws Exception {
        test.operation = Operation.STOP_TEST_CONTEXT;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.testContext.isStopped());
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    @Test
    public void testTimeStep_withOperation_shouldThrowException() throws Exception {
        test.operation = Operation.CALL_TIMESTEP_WITH_ENUM;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        for (int i = 1; i <= THREAD_COUNT; i++) {
            assertTrue(new File(i + ".exception").exists());
        }
    }

    private static class WorkerTest {

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
                    case CALL_TIMESTEP_WITH_ENUM:
                        timeStep(operation);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation: " + operation);
                }
            }
        }
    }
}
