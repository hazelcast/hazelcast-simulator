package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.fake.FakeInstance;
import org.junit.After;
import org.junit.Before;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static org.mockito.Mockito.mock;

public abstract class TestContainer_AbstractTest {

    TestContextImpl testContext = new TestContextImpl("TestContainerTest", "localhost", mock(Server.class));

    TestContainer testContainer;

    File userDir;

    @Before
    public void before() {
        ExceptionReporter.reset();
        userDir = setupFakeUserDir();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    <T> TestContainer createTestContainer(T test) {
        return new TestContainer(testContext, test, new TestCase("foo"), mock(FakeInstance.class));
    }

    <T> TestContainer createTestContainer(T test, FakeInstance hz) {
        return new TestContainer(testContext, test, new TestCase("foo"), hz);
    }

    <T> TestContainer createTestContainer(T test, TestCase testCase) {
        return new TestContainer(testContext, test, testCase, mock(FakeInstance.class));
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
