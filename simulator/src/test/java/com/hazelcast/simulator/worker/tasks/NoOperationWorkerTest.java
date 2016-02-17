package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContextImpl;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
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
        testContainer = new TestContainer(testContext, test, THREAD_COUNT);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRun() throws Exception {
        testContainer.invoke(TestPhase.RUN);

        assertEquals(THREAD_COUNT + 1, test.workerCreated);
    }

    private static class WorkerTest {

        private volatile int workerCreated;

        @RunWithWorker
        public IWorker createWorker() {
            workerCreated++;
            return new NoOperationWorker();
        }
    }
}
