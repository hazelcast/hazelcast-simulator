package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.common.FailureType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.common.FailureType.WORKER_FINISHED;
import static com.hazelcast.simulator.common.FailureType.WORKER_OOM;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FailureCollectorTest {

    private FailureCollector failureCollector;

    private FailureOperation exceptionOperation;
    private FailureOperation oomOperation;
    private FailureOperation finishedOperation;
    private File outputDirectory;

    @Before
    public void setUp() {
        outputDirectory = TestUtils.createTmpDirectory();
        failureCollector = new FailureCollector(outputDirectory);

        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        String agentAddress = workerAddress.getParent().toString();

        exceptionOperation = new FailureOperation("exception", WORKER_EXCEPTION, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        oomOperation = new FailureOperation("oom", WORKER_OOM, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);

        finishedOperation = new FailureOperation("finished", WORKER_FINISHED, workerAddress, agentAddress,
                "127.0.0.1:5701", "workerId", "testId", null, null);
    }

    @After
    public void tearDown() {
        deleteQuiet(outputDirectory);
    }

    @Test
    public void testAddFailureOperation_withException() {
        assertEquals(0, failureCollector.getFailureCount());

        failureCollector.notify(exceptionOperation);

        assertEquals(1, failureCollector.getFailureCount());
    }

    @Test
    public void testAddFailureOperation_withWorkerFinishedFailure() {
        assertEquals(0, failureCollector.getFailureCount());

        failureCollector.notify(oomOperation);

        assertEquals(1, failureCollector.getFailureCount());
    }

    @Test
    public void testAddFailureOperation_withPoisonPill() {
        assertEquals(0, failureCollector.getFailureCount());

        failureCollector.notify(finishedOperation);

        assertEquals(0, failureCollector.getFailureCount());
    }

    @Test
    public void testHasCriticalFailure() {
        failureCollector.notify(exceptionOperation);
        assertTrue(failureCollector.hasCriticalFailure());
    }

    @Test
    public void testLogFailureInfo_noFailures() {
        failureCollector.logFailureInfo();
    }

    @Test(expected = CommandLineExitException.class)
    public void testLogFailureInfo_withFailures() {
        failureCollector.notify(exceptionOperation);
        failureCollector.logFailureInfo();
    }
}
