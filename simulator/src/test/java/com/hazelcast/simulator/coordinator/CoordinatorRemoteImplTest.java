package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.coordinator.operations.RcDownloadOperation;
import com.hazelcast.simulator.coordinator.operations.RcInstallOperation;
import com.hazelcast.simulator.coordinator.operations.RcPrintLayoutOperation;
import com.hazelcast.simulator.coordinator.operations.RcStopCoordinatorOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStatusOperation;
import com.hazelcast.simulator.coordinator.operations.RcTestStopOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerKillOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerScriptOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorRemoteImplTest {

    private Coordinator coordinator;
    private CoordinatorRemoteImpl remote;

    @Before
    public void before() {
        coordinator = mock(Coordinator.class);
        remote = new CoordinatorRemoteImpl(coordinator);
    }

    @Test
    public void test_RcDownloadOperation() throws Exception {
        RcDownloadOperation op = new RcDownloadOperation();

        String result = remote.execute(op);

        assertNull(result);
        verify(coordinator).download();
    }

    @Test
    public void test_RcInstallOperation() throws Exception {
        RcInstallOperation op = new RcInstallOperation("maven=3.8");

        String result = remote.execute(op);

        assertNull(result);
        verify(coordinator).installVendor(op.getVersionSpec());
    }

    @Test
    public void test_RcPrintLayoutOperation() throws Exception {
        RcPrintLayoutOperation op = new RcPrintLayoutOperation();
        String expected = "somelayout";
        when(coordinator.printLayout()).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test
    public void test_RcStopCoordinatorOperation() throws Exception {
        RcStopCoordinatorOperation op = new RcStopCoordinatorOperation();

        String result = remote.execute(op);

        assertNull(result);
        verify(coordinator).close();
    }

    @Test
    public void test_RcTestRunOperation() throws Exception {
        TestSuite suite = mock(TestSuite.class);
        RcTestRunOperation op = new RcTestRunOperation(suite);

        String expected = "test";
        when(coordinator.testRun(op)).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test
    public void test_RcTestStatusOperation() throws Exception {
        RcTestStatusOperation op = new RcTestStatusOperation("testId");

        String expected = "ready";
        when(coordinator.testStatus(op)).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test
    public void test_RcTestStopOperation() throws Exception {
        RcTestStopOperation op = new RcTestStopOperation("testId");

        String expected = "ready";
        when(coordinator.testStop(op)).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test
    public void test_RcWorkerKillOperation() throws Exception {
        RcWorkerKillOperation op = new RcWorkerKillOperation("bla", mock(WorkerQuery.class));

        String expected = "ok";
        when(coordinator.workerKill(op)).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test
    public void test_RcWorkerScriptOperation() throws Exception {
        RcWorkerScriptOperation op = new RcWorkerScriptOperation("bla");

        String expected = "ok";
        when(coordinator.workerScript(op)).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test
    public void test_RcWorkerStartOperation() throws Exception {
        RcWorkerStartOperation op = new RcWorkerStartOperation();

        String expected = "ok";
        when(coordinator.workerStart(op)).thenReturn(expected);

        String result = remote.execute(op);

        assertSame(expected, result);
    }

    @Test(expected = ProcessException.class)
    public void test_UnknownOperation() throws Exception {
        CreateWorkerOperation op = mock(CreateWorkerOperation.class);

        remote.execute(op);
    }
}
