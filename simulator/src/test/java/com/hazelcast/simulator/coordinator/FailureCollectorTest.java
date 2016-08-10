package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.TestUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.common.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.common.FailureType.WORKER_TIMEOUT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class FailureCollectorTest {

    private final static int FINISHED_WORKER_TIMEOUT_SECONDS = 120;

    private ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
    private FailureCollector failureCollector;

    private FailureOperation exceptionOperation;
    private FailureOperation oomOperation;
    private FailureOperation finishedOperation;
    private FailureOperation nonCriticalOperation;
    private File outputDirectory;

    @Before
    public void setUp() {
        outputDirectory = TestUtils.createTmpDirectory();
        failureCollector = new FailureCollector(
                outputDirectory, componentRegistry, singleton(WORKER_TIMEOUT));

        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        String agentAddress = workerAddress.getParent().toString();

        exceptionOperation = new FailureOperation("exception", WORKER_EXCEPTION, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        oomOperation = new FailureOperation("oom", WORKER_OOM, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        finishedOperation = new FailureOperation("finished", WORKER_FINISHED, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        nonCriticalOperation = new FailureOperation("timeout", WORKER_TIMEOUT, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);
    }

    @After
    public void tearDown() {
        deleteQuiet(outputDirectory);
    }

    @Test
    public void testAddFailureOperation_withException() {
        assertEquals(0, failureCollector.getFailureCount());
        assertEquals(0, failureCollector.getFinishedWorkers().size());

        failureCollector.addFailureOperation(exceptionOperation);

        assertEquals(1, failureCollector.getFailureCount());
        assertEquals(0, failureCollector.getFinishedWorkers().size());
    }

    @Test
    public void testAddFailureOperation_withWorkerFinishedFailure() {
        assertEquals(0, failureCollector.getFailureCount());
        assertEquals(0, failureCollector.getFinishedWorkers().size());

        failureCollector.addFailureOperation(oomOperation);

        assertEquals(1, failureCollector.getFailureCount());
        assertEquals(1, failureCollector.getFinishedWorkers().size());
    }

    @Test
    public void testAddFailureOperation_withPoisonPill() {
        assertEquals(0, failureCollector.getFailureCount());
        assertEquals(0, failureCollector.getFinishedWorkers().size());

        failureCollector.addFailureOperation(finishedOperation);

        assertEquals(0, failureCollector.getFailureCount());
        assertEquals(1, failureCollector.getFinishedWorkers().size());
    }

    @Test
    public void testHasCriticalFailure() {
        failureCollector.addFailureOperation(exceptionOperation);
        assertTrue(failureCollector.hasCriticalFailure());
    }

    @Test
    public void testHasCriticalFailure_withNonCriticalFailures() {
        Set<FailureType> nonCriticalFailures = singleton(exceptionOperation.getType());
        failureCollector = new FailureCollector(outputDirectory, componentRegistry, nonCriticalFailures);

        failureCollector.addFailureOperation(exceptionOperation);
        assertFalse(failureCollector.hasCriticalFailure());
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown() {
        addFinishedWorker(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0));

        ThreadSpawner spawner = new ThreadSpawner("testWaitForFinishedWorker", true);
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                addFinishedWorker(new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0));
                sleepSeconds(1);
                addFinishedWorker(new SimulatorAddress(AddressLevel.WORKER, 1, 3, 0));
            }
        });

        boolean success = failureCollector.waitForWorkerShutdown(3, FINISHED_WORKER_TIMEOUT_SECONDS);
        assertTrue(success);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown_withTimeout() {
        addFinishedWorker(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0));

        boolean success = failureCollector.waitForWorkerShutdown(3, 1);
        assertFalse(success);
    }

    @Test
    public void testLogFailureInfo_noFailures() {
        failureCollector.logFailureInfo();
    }

    @Test
    public void testLogFailureInfo_withNonCriticalFailures() {
        failureCollector.addFailureOperation(nonCriticalOperation);
        failureCollector.logFailureInfo();
    }

    @Test(expected = CommandLineExitException.class)
    public void testLogFailureInfo_withFailures() {
        failureCollector.addFailureOperation(exceptionOperation);
        failureCollector.logFailureInfo();
    }

    private void addFinishedWorker(SimulatorAddress workerAddress) {
        FailureOperation operation = new FailureOperation("finished", WORKER_FINISHED, workerAddress,
                workerAddress.getParent().toString(), "127.0.0.1:5701", "workerId", "testId", null, null);
        failureCollector.addFailureOperation(operation);
    }
}
