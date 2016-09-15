package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.StartTestOperation;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.tests.SuccessTimeStepTest;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.common.TestPhase.WARMUP;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestContainerManagerTest {

    private ServerConnector connector = mock(ServerConnector.class);
    private TestContainerManager containerManager;
    private SimulatorAddress workerAddress;
    private SimulatorAddress testAddress;

    @Before
    public void before() {
        setupFakeUserDir();

        HazelcastInstance hz = mock(HazelcastInstance.class);
        when(hz.getUserContext()).thenReturn(new ConcurrentHashMap<String, Object>());

        workerAddress = new SimulatorAddress(AddressLevel.WORKER, 0, 1, 0);
        containerManager = new TestContainerManager(hz, workerAddress, "127.0.0.1", connector, WorkerType.MEMBER);
        testAddress = new SimulatorAddress(AddressLevel.TEST, 0, 0, 1);
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test
    public void allCycles() throws Exception {
        final TestContainer container = createTest(SuccessTimeStepTest.class);

        for (TestPhase phase : TestPhase.values()) {
            if (phase == RUN || phase == WARMUP) {
                startRunOrWarmup(phase == WARMUP);
                assertStateEventually(container, phase);

                sleepSeconds(2);

                containerManager.stop(testAddress);
                assertStateEventually(container, null);
            } else {
                executePhase(phase);
            }
        }
    }

    @Test
    public void whenTestCompletes_thenRemoved() throws Exception {
        createTest(SuccessTest.class);
        executePhase(TestPhase.getLastTestPhase());

        assertNull(containerManager.get(testAddress.getTestIndex()));
    }

    @Test
    public void whenErrorDuringRun() throws Exception {
        TestContainer container = createTest(FailingTest.class);
        executePhase(SETUP);

        startRunOrWarmup(false);

        assertStateEventually(container, null);

    }

    private void assertStateEventually(final TestContainer container, final TestPhase state) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                TestPhase testPhase = container.getTestPhase();
                assertEquals(state, testPhase);
            }
        });
    }

    private Future startRunOrWarmup(boolean warmup) {
        StartTestOperation operation = new StartTestOperation(TargetType.MEMBER, new LinkedList<String>(), warmup);
        return containerManager.start(operation, testAddress);
    }

    private void executePhase(TestPhase phase) throws Exception {
        containerManager.startTestPhase(testAddress, phase).get();
    }

    private TestContainer createTest(Class clazz) {
        CreateTestOperation createTestOperation = new CreateTestOperation(testAddress.getTestIndex(),
                new TestCase("foo").setProperty("class", clazz));

        containerManager.createTest(createTestOperation);

        return containerManager.get(testAddress.getTestIndex());
    }

    @Test
    public void stop_whenNonExistingTest_thenIgnored() {
        containerManager.stop(new SimulatorAddress(AddressLevel.TEST, 0, 0, 100));
    }

    @Test(expected = ProcessException.class)
    public void startTestPhase_whenNonExistingTest() throws Exception {
        containerManager.startTestPhase(new SimulatorAddress(AddressLevel.TEST, 0, 0, 100), TestPhase.SETUP);
    }

    @Test(expected = IllegalStateException.class)
    public void startTestPhase_whenOtherPhaseBusy() throws Exception {
        final TestContainer container = createTest(SuccessTest.class);

        executePhase(TestPhase.SETUP);
        startRunOrWarmup(false);
        assertStateEventually(container, RUN);

        containerManager.startTestPhase(testAddress, TestPhase.LOCAL_TEARDOWN);
    }

    @Test(expected = ProcessException.class)
    public void start_whenNonExistingTest() throws Exception {
        containerManager.start(new StartTestOperation(TargetType.MEMBER), new SimulatorAddress(AddressLevel.TEST, 0, 0, 100));
    }


//    @Test
//    public void process_StartTest_failingTest() throws Exception {
//        createTestOperationProcessor(FailingTest.class);
//
//        runPhase(TestPhase.SETUP);
//        runTest();
//
//        //exceptionLogger.assertException(TestException.class);
//    }

//    @Test
//    public void process_StartTest_skipRunPhase_targetTypeMismatch() throws Exception {
//        createTestOperationProcessor();
//
//        StartTestOperation operation = new StartTestOperation(TargetType.CLIENT);
//        ResponseType responseType = process(processor, operation, COORDINATOR);
//        assertEquals(SUCCESS, responseType);
//
//        waitForPhaseCompletion(TestPhase.RUN);
//
//        //exceptionLogger.assertNoException();
//    }
//
//    @Test
//    public void process_StartTest_skipRunPhase_notOnTargetWorkersList() throws Exception {
//        createTestOperationProcessor();
//
//        List<String> targetWorkers = singletonList(new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0).toString());
//        StartTestOperation operation = new StartTestOperation(TargetType.ALL, targetWorkers, false);
//        ResponseType responseType = process(processor, operation, COORDINATOR);
//        assertEquals(SUCCESS, responseType);
//
//        waitForPhaseCompletion(TestPhase.RUN);
//
//        //exceptionLogger.assertNoException();
//    }


}
