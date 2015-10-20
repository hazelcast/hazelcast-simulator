package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.FailureType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Set;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class FailureContainerTest {

    private ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
    private FailureContainer failureContainer = new FailureContainer("testSuite", componentRegistry);

    private FailureOperation exceptionOperation;
    private FailureOperation oomOperation;
    private FailureOperation finishedOperation;

    @Before
    public void setUp() {
        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        String agentAddress = workerAddress.getParent().toString();

        exceptionOperation = new FailureOperation("exception", FailureType.WORKER_EXCEPTION, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        oomOperation = new FailureOperation("oom", FailureType.WORKER_OOM, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        finishedOperation = new FailureOperation("finished", FailureType.WORKER_FINISHED, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);
    }

    @After
    public void tearDown() {
        deleteQuiet(new File("failures-testSuite.txt"));
    }

    @Test
    public void testAddFailureOperation_withException() {
        assertEquals(0, failureContainer.getFailureCount());
        assertEquals(0, failureContainer.getFinishedWorkers().size());

        failureContainer.addFailureOperation(exceptionOperation);

        assertEquals(1, failureContainer.getFailureCount());
        assertEquals(0, failureContainer.getFinishedWorkers().size());
    }

    @Test
    public void testAddFailureOperation_withWorkerFinishedFailure() {
        assertEquals(0, failureContainer.getFailureCount());
        assertEquals(0, failureContainer.getFinishedWorkers().size());

        failureContainer.addFailureOperation(oomOperation);

        assertEquals(1, failureContainer.getFailureCount());
        assertEquals(1, failureContainer.getFinishedWorkers().size());
    }

    @Test
    public void testAddFailureOperation_withPoisonPill() {
        assertEquals(0, failureContainer.getFailureCount());
        assertEquals(0, failureContainer.getFinishedWorkers().size());

        failureContainer.addFailureOperation(finishedOperation);

        assertEquals(0, failureContainer.getFailureCount());
        assertEquals(1, failureContainer.getFinishedWorkers().size());
    }

    @Test
    public void testHasCriticalFailure() {
        failureContainer.addFailureOperation(exceptionOperation);

        Set<FailureType> nonCriticalFailures = Collections.singleton(exceptionOperation.getType());
        assertFalse(failureContainer.hasCriticalFailure(nonCriticalFailures));

        assertTrue(failureContainer.hasCriticalFailure(Collections.<FailureType>emptySet()));
    }
}
