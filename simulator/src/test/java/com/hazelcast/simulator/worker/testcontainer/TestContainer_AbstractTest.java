package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import org.junit.After;
import org.junit.Before;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static org.mockito.Mockito.mock;

public abstract class TestContainer_AbstractTest {

    TestContextImpl testContext = new TestContextImpl(
            mock(HazelcastInstance.class), "TestContainerTest","localhost",mock(WorkerConnector.class));

    TestContainer testContainer;

    File userDir;

    @Before
    public void setup() {
        ExceptionReporter.reset();
        userDir = setupFakeUserDir();
    }

    @After
    public void tearDown() {
        teardownFakeUserDir();
    }

    <T> TestContainer createTestContainer(T test) {
        return new TestContainer(testContext, test, new TestCase("foo"));
    }


    <T> TestContainer createTestContainer(T test, TestCase testCase) {
        return new TestContainer(testContext, test, testCase);
    }

    public static class BaseTest {

        boolean runCalled;

        @Run
        public void run() {
            runCalled = true;
        }
    }

    public static class BaseSetupTest extends BaseTest {

        TestContext context;
        boolean setupCalled;

        @Setup
        public void setUp(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }
    }
}
