package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.core.ICompletableFuture;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractAsyncWorkerTest {

    private static final int THREAD_COUNT = 3;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private enum Operation {
        EXCEPTION,
        ON_RESPONSE,
        ON_FAILURE
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        test = new WorkerTest();
        testContext = new TestContextImpl("AbstractAsyncWorkerTest");
        testContainer = new TestContainer(testContext, test, THREAD_COUNT);

        ExceptionReporter.reset();
    }

    @After
    public void tearDown() {
        try {
            for (int i = 1; i <= THREAD_COUNT; i++) {
                deleteQuiet(i + ".exception");
            }

            ExceptionReporter.reset();
        } finally {
            test.executor.shutdown();
        }
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
    public void testRun_onResponse() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.ON_RESPONSE);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);
        test.responseLatch.await();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRun_onFailure() throws Exception {
        test.operationSelectorBuilder.addDefaultOperation(Operation.ON_FAILURE);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);
        test.failureLatch.await();

        for (int i = 1; i <= THREAD_COUNT; i++) {
            assertTrue(new File(i + ".exception").exists());
        }
        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    private static class WorkerTest {

        private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
        private final ExecutorService executor = createFixedThreadPool(THREAD_COUNT * 2, "AbstractAsyncWorkerTest");

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
            return new Worker(workerCreated++);
        }

        private class Worker extends AbstractAsyncWorker<Operation, String> {

            private final int workerId;

            Worker(int workerId) {
                super(operationSelectorBuilder);
                this.workerId = workerId;
            }

            @Override
            protected void timeStep(final Operation operation) throws Exception {
                ICompletableFuture<String> future;
                switch (operation) {
                    case EXCEPTION:
                        throw new TestException("expected exception");
                    case ON_RESPONSE:
                        future = new CompletedFuture<String>(null, "test" + workerId, executor);
                        break;
                    case ON_FAILURE:
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

                stopWorker();
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
