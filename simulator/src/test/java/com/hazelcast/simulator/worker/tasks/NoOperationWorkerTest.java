package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.testcontainer.TestContainer;
import com.hazelcast.simulator.testcontainer.TestContextImpl;
import com.hazelcast.simulator.testcontainer.TestPhase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NoOperationWorkerTest {

    private static final int THREAD_COUNT = 3;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private WorkerTest test;
    private TestContainer testContainer;

    @Before
    public void setUp() {
        test = new WorkerTest();
        TestContextImpl testContext = new TestContextImpl("AbstractWorkerTest");
        TestCase testCase = new TestCase("id").setProperty("threadCount", THREAD_COUNT);
        testContainer = new TestContainer(testContext, test, testCase);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRun() throws Exception {
        testContainer.invoke(TestPhase.RUN);

        assertEquals(THREAD_COUNT, test.workerCreated);
    }

    public static class WorkerTest {

        private volatile int workerCreated;

        @RunWithWorker
        public IWorker createWorker() {
            workerCreated++;
            return new NoOperationWorker();
        }
    }
}
