package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class NoOperationWorkerTest {

    private static final int THREAD_COUNT = 3;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private WorkerTest test;
    private TestContainer testContainer;
    private File userDir;

    @Before
    public void before() {
        userDir = setupFakeUserDir();

        test = new WorkerTest();
        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), "Test", "localhost", mock(WorkerConnector.class));
        TestCase testCase = new TestCase("id").setProperty("threadCount", THREAD_COUNT);
        testContainer = new TestContainer(testContext, test, testCase);
    }

    @After
    public void after(){
        FileUtils.deleteQuiet(userDir);
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
