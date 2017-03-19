package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.DummyPromise;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.StoppingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.tests.TestWithSlowSetup;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.vendors.VendorDriver;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import com.hazelcast.simulator.worker.operations.StartPhaseOperation;
import com.hazelcast.simulator.worker.operations.StopRunOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collection;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestManagerTest {

    private TestManager manager;
    private Server server;
    private VendorDriver vendorDriver;
    private File userDir;

    @Before
    public void before() throws Exception {
        ExceptionReporter.reset();
        userDir = setupFakeUserDir();
        server = mock(Server.class);
        vendorDriver = mock(VendorDriver.class);
        when(vendorDriver.getInstance()).thenReturn(mock(HazelcastInstance.class));
        manager = new TestManager(server, vendorDriver);
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test
    public void test_createTest() {
        TestCase testCase = new TestCase("foo")
                .setProperty("class", SuccessTest.class);
        CreateTestOperation op = new CreateTestOperation(testCase);

        manager.createTest(op);

        Collection<TestContainer> containers = manager.getContainers();
        assertEquals(1, containers.size());
    }

    @Test(expected = IllegalStateException.class)
    public void test_createTest_whenTestExist() {
        TestCase testCase = new TestCase("foo")
                .setProperty("class", SuccessTest.class);
        CreateTestOperation op = new CreateTestOperation(testCase);

        manager.createTest(op);

        // duplicate.
        manager.createTest(op);
    }

    @Test
    public void test_startRun() throws Exception {
        TestCase testCase = new TestCase("foo")
                .setProperty("threadCount", 1)
                .setProperty("class", StoppingTest.class);

        manager.createTest(new CreateTestOperation(testCase));

        DummyPromise promise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(RUN, "foo"), promise);

        promise.assertCompletesEventually();
    }

    @Test
    public void test_startPhase_whenPreviousPhaseStillRunning() throws Exception {
        TestCase testCase = new TestCase("foo")
                .setProperty("threadCount", 1)
                .setProperty("class", SuccessTest.class);

        manager.createTest(new CreateTestOperation(testCase));
        final TestContainer container = manager.getContainers().iterator().next();

        // do the setup first (needed)
        DummyPromise setupPromise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(SETUP, "foo"), setupPromise);
        setupPromise.assertCompletesEventually();

        // then start with the run phase
        manager.startTestPhase(new StartPhaseOperation(RUN, "foo"), mock(Promise.class));
        awaitRunning(container);

        // and while the run phase is running, we'll try to do a tear down
        DummyPromise promise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(LOCAL_TEARDOWN, "foo"), promise);
        promise.assertCompletesEventually();
        assertTrue(promise.getAnswer() instanceof IllegalStateException);
    }

    private void awaitRunning(final TestContainer container) {
        // wait till the test starts running.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(RUN, container.getCurrentPhase());
            }
        });
    }

    @Test
    public void test_stopRun() throws Exception {
        TestCase testCase = new TestCase("foo")
                .setProperty("threadCount", 1)
                .setProperty("class", SuccessTest.class);

        manager.createTest(new CreateTestOperation(testCase));
        final TestContainer container = manager.getContainers().iterator().next();

        // we need to call setup so the test is initialized correctly
        DummyPromise setupPromise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(SETUP, "foo"), setupPromise);
        setupPromise.assertCompletesEventually();

        // then we call start; this call will not block
        DummyPromise runPromise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(RUN, "foo"), runPromise);

        awaitRunning(container);

        // then we eventually call stop
        manager.stopRun(new StopRunOperation("foo"));

        // and now the test should complete.
        runPromise.assertCompletesEventually();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_stopRun_whenNotExistingTest() {
        manager.stopRun(new StopRunOperation("foo"));
    }

    @Test
    public void test_startTestPhase() throws Exception {
        TestCase testCase = new TestCase("foo")
                .setProperty("threadCount", 1)
                .setProperty("class", TestWithSlowSetup.class);

        manager.createTest(new CreateTestOperation(testCase));

        // then we call start; this call will not block
        DummyPromise promise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(SETUP, "foo"), promise);

        // give the test some time to enter the setup phase
        SECONDS.sleep(1);

        assertFalse(promise.hasAnswer());

        // but eventually the promise will complete.
        promise.assertCompletesEventually();
    }

    @Test
    public void test_whenProblemDuringPhase() throws Exception {
        TestCase testCase = new TestCase("foo")
                .setProperty("threadCount", 1)
                .setProperty("class", FailingTest.class);

        manager.createTest(new CreateTestOperation(testCase));

        DummyPromise setupPromise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(SETUP, "foo"), setupPromise);
        setupPromise.assertCompletesEventually();

        DummyPromise runPromise = new DummyPromise();
        manager.startTestPhase(new StartPhaseOperation(RUN, "foo"), runPromise);

        runPromise.assertCompletesEventually();
        System.out.println(runPromise.getAnswer());
        assertTrue(runPromise.getAnswer() instanceof Exception);
    }

    @Test
    public void test_whenLastPhaseCompletes_thenTestRemoved() throws Exception {
        final TestCase testCase = new TestCase("foo")
                .setProperty("threadCount", 1)
                .setProperty("class", TestWithSlowSetup.class);

        manager.createTest(new CreateTestOperation(testCase));

        // then we call start; this call will not block
        DummyPromise promise = new DummyPromise();

        manager.startTestPhase(new StartPhaseOperation(TestPhase.LOCAL_TEARDOWN, "foo"), promise);
        // but eventually the promise will complete.
        promise.assertCompletesEventually();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, manager.getContainers().size());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_startTestPhase_whenNonExistingTest() throws Exception {
        manager.startTestPhase(new StartPhaseOperation(SETUP, "foo"), mock(Promise.class));
    }
}
