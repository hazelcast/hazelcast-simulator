package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerData;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;

import static com.hazelcast.simulator.common.FailureType.WORKER_ABNORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_NORMAL_EXIT;
import static com.hazelcast.simulator.common.FailureType.WORKER_OOME;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.workerAddress;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FailureCollectorTest {

    private FailureCollector failureCollector;

    private FailureOperation exceptionFailure;
    private FailureOperation oomeFailure;
    private FailureOperation normalExitFailure;
    private FailureOperation abnormalExitFailure;
    private File outputDirectory;
    private Registry registry;
    private SimulatorAddress agentAddress;
    private SimulatorAddress workerAddress;

    @Before
    public void before() {
        outputDirectory = TestUtils.createTmpDirectory();
        registry = new Registry();
        failureCollector = new FailureCollector(outputDirectory, registry);

        agentAddress = registry.addAgent("192.168.0.1", "192.168.0.1").getAddress();

        workerAddress = workerAddress(agentAddress.getAgentIndex(), 1);

        WorkerParameters workerParameters = new WorkerParameters()
                .set("WORKER_ADDRESS", workerAddress);

        registry.addWorkers(singletonList(workerParameters));

        exceptionFailure = new FailureOperation("exception", WORKER_EXCEPTION, workerAddress, agentAddress.toString(),
                "workerId", "testId", null);

        abnormalExitFailure = new FailureOperation("exception", WORKER_ABNORMAL_EXIT, workerAddress, agentAddress.toString(),
                "workerId", "testId", null);

        oomeFailure = new FailureOperation("oom", WORKER_OOME, workerAddress, agentAddress.toString(),
               "workerId", "testId", null);

        normalExitFailure = new FailureOperation("finished", WORKER_NORMAL_EXIT, workerAddress, agentAddress.toString(),
               "workerId", "testId", null);
    }

    @After
    public void after() {
        deleteQuiet(outputDirectory);
    }

    @Test
    public void notify_whenNonExistingWorker_thenIgnore() {
        SimulatorAddress nonExistingWorkerAddress = workerAddress(agentAddress.getAgentIndex(), 100);
        FailureOperation failure = new FailureOperation("exception", WORKER_EXCEPTION, nonExistingWorkerAddress, agentAddress.toString(),
                "workerId", "testId", null);

        failureCollector.notify(failure);

        assertEquals(0, failureCollector.getFailureCount());
    }

    @Test
    public void notify_whenWorkerIgnoresFailure_thenIgnore() {
        notify_whenWorkerIgnoresFailure(oomeFailure, true);
    }

    @Test
    public void notify_whenWorkerIgnoresFailure_andNormalExitFailure() {
        notify_whenWorkerIgnoresFailure(normalExitFailure, true);
    }

    @Test
    public void notify_whenWorkerIgnoresFailure_andAbnormalExitFailure() {
        notify_whenWorkerIgnoresFailure(abnormalExitFailure, true);
    }

    @Test
    public void notify_whenWorkerIgnoresFailure_andExceptionalFailure() {
        notify_whenWorkerIgnoresFailure(exceptionFailure, false);
    }

    private void notify_whenWorkerIgnoresFailure(FailureOperation failure, boolean workerDeleted) {
        WorkerData worker = registry.getWorker(workerAddress);
        worker.setIgnoreFailures(true);

        failureCollector.notify(failure);

        assertEquals(0, failureCollector.getFailureCount());
        assertEquals(workerDeleted ? null : worker, registry.getWorker(workerAddress));
    }

    @Test
    public void notify_enrich() {
        TestCase testCase = new TestCase("test1");
        TestSuite suite1 = new TestSuite().addTest(testCase);
        TestSuite suite2 = new TestSuite().addTest(new TestCase("test2"));

        registry.addTests(suite1);
        registry.addTests(suite2);

        FailureOperation failure = new FailureOperation("exception", WORKER_EXCEPTION, workerAddress, agentAddress.toString(),
                "workerId", testCase.getId(), null);

        FailureListener listener = mock(FailureListener.class);
        failureCollector.addListener(listener);
        failureCollector.notify(failure);

        ArgumentCaptor<FailureOperation> failureCaptor = ArgumentCaptor.forClass(FailureOperation.class);
        verify(listener).onFailure(failureCaptor.capture(), eq(false), eq(true));

        assertSame(suite1.getTestCaseList().get(0), failureCaptor.getValue().getTestCase());
    }

    @Test
    public void notify_withException() {
        failureCollector.notify(exceptionFailure);

        assertEquals(1, failureCollector.getFailureCount());
    }

    @Test
    public void notify_withWorkerFinishedFailure() {
        failureCollector.notify(oomeFailure);

        assertEquals(1, failureCollector.getFailureCount());
    }

    @Test
    public void notify_withPoisonPill() {
        failureCollector.notify(normalExitFailure);

        assertEquals(0, failureCollector.getFailureCount());
    }

    @Test
    public void testHasCriticalFailure() {
        failureCollector.notify(exceptionFailure);
        assertTrue(failureCollector.hasCriticalFailure());
    }

    @Test
    public void testLogFailureInfo_noFailures() {
        failureCollector.logFailureInfo();
    }

    @Test
    public void testLogFailureInfo_withFailures() {
        failureCollector.notify(exceptionFailure);
        failureCollector.logFailureInfo();
    }
}
