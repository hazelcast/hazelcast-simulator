package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.core.ICompletableFuture;
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
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.executor.CompletedFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractAsyncWorkerTest {

    private static final int THREAD_COUNT = 3;

    private enum Operation {
        RESPONSE,
        EXCEPTION
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
        testContext = new TestContextImpl("AbstractAsyncWorkerTest", null);
        TestCase testCase = new TestCase("AbstractAsyncWorkerTest", properties);
        testContainer = new TestContainer(test, testContext, testCase);

        ExceptionReporter.reset();
    }

    @After
    public void tearDown() throws Exception {
        for (int i = 1; i <= THREAD_COUNT; i++) {
            deleteQuiet(new File(i + ".exception"));
        }

        ExceptionReporter.reset();
        test.executor.shutdown();
    }

    @Test
    public void testInvokeSetup() throws Exception {
        testContainer.invoke(TestPhase.SETUP);

        assertEquals(testContext, test.testContext);
        assertEquals(0, test.workerCreated);
    }

    @Test(timeout = 10000)
    public void testRun() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.RESPONSE);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);
        test.responseLatch.await();
    }

    @Test(timeout = 10000)
    public void testRun_withException() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.EXCEPTION);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);
        test.failureLatch.await();

        for (int i = 1; i <= THREAD_COUNT; i++) {
            assertTrue(new File(i + ".exception").exists());
        }
    }

    private static class WorkerTest {

        private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
        private final ExecutorService executor = createFixedThreadPool(THREAD_COUNT, "AbstractAsyncWorkerTest");

        private final CountDownLatch responseLatch = new CountDownLatch(THREAD_COUNT);
        private final CountDownLatch failureLatch = new CountDownLatch(THREAD_COUNT);

        private TestContext testContext;

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

        private class Worker extends AbstractAsyncWorker<Operation, String> {

            Worker() {
                super(operationSelectorBuilder);
            }

            @Override
            protected void timeStep(final Operation operation) throws Exception {
                ICompletableFuture<String> future;
                switch (operation) {
                    case RESPONSE:
                        future = new CompletedFuture<String>(null, "test", executor);
                        break;
                    case EXCEPTION:
                        future = new CompletedFuture<String>(null, new TestException("expected exception"), executor);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation: " + operation);
                }
                future.andThen(this);

                try {
                    future.get();
                } catch (Exception e) {
                    EmptyStatement.ignore(e);
                }

                stopTestContext();
            }

            @Override
            protected void handleResponse(String response) {
                responseLatch.countDown();
            }

            @Override
            protected void handleFailure(Throwable t) {
                failureLatch.countDown();
            }
        }
    }
}
