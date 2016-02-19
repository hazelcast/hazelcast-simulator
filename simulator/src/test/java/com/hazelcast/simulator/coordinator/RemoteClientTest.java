package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.PingOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteClientTest {

    private static final int WORKER_PING_INTERVAL_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    private static final int MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS = 0;
    private static final IntegrationTestOperation DEFAULT_OPERATION = new IntegrationTestOperation();

    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private final CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);
    private final ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
    private final WorkerParameters workerParameters = mock(WorkerParameters.class);

    @Before
    public void setUp() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
        when(workerJvmSettings.getWorkerIndex()).thenReturn(1);

        SimulatorAddress agentAddress = componentRegistry.getFirstAgent().getAddress();
        componentRegistry.addWorkers(agentAddress, Collections.singletonList(workerJvmSettings));

        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);
    }

    @Test
    public void testLogOnAllAgents() {
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.logOnAllAgents("test");

        verify(coordinatorConnector).write(eq(ALL_AGENTS), any(LogOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testLogOnAllWorkers() {
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.logOnAllWorkers("test");

        verify(coordinatorConnector).write(eq(ALL_WORKERS), any(LogOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testCreateWorkers_withClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 3);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.createWorkers(clusterLayout, false);
    }

    @Test
    public void testCreateWorkers_noClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.createWorkers(clusterLayout, false);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() {
        initMockForCreateWorkerOperation(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.createWorkers(clusterLayout, false);
    }

    @Test(expected = SimulatorProtocolException.class)
    public void testCreateWorkers_withExceptionOnWrite() {
        initMockForCreateWorkerOperation(null);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.createWorkers(clusterLayout, false);
    }

    @Test
    public void testCreateWorkersAndTerminateWorkers_withPokeThread() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.createWorkers(clusterLayout, true);

        sleepSeconds(1);

        remoteClient.terminateWorkers(true);
    }

    @Test
    public void testSendToAllAgents() {
        initMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        remoteClient.sendToAllAgents(DEFAULT_OPERATION);

        verify(coordinatorConnector).write(eq(ALL_AGENTS), eq(DEFAULT_OPERATION));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToAllAgents_withErrorResponse() {
        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        try {
            remoteClient.sendToAllAgents(DEFAULT_OPERATION);
        } finally {
            verify(coordinatorConnector).write(eq(ALL_AGENTS), eq(DEFAULT_OPERATION));
            verifyNoMoreInteractions(coordinatorConnector);
        }
    }

    @Test
    public void testSendToAllWorkers() {
        initMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        remoteClient.sendToAllWorkers(DEFAULT_OPERATION);

        verify(coordinatorConnector).write(eq(ALL_WORKERS), eq(DEFAULT_OPERATION));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToAllWorkers_withErrorResponse() {
        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        try {
            remoteClient.sendToAllWorkers(DEFAULT_OPERATION);
        } finally {
            verify(coordinatorConnector).write(eq(ALL_WORKERS), eq(DEFAULT_OPERATION));
            verifyNoMoreInteractions(coordinatorConnector);
        }
    }

    @Test
    public void testSendToFirstWorker() {
        initMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();

        remoteClient.sendToFirstWorker(DEFAULT_OPERATION);

        verify(coordinatorConnector).write(eq(firstWorkerAddress), eq(DEFAULT_OPERATION));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToFirstWorker_withErrorResponse() {
        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();

        try {
            remoteClient.sendToFirstWorker(DEFAULT_OPERATION);
        } finally {
            verify(coordinatorConnector).write(eq(firstWorkerAddress), eq(DEFAULT_OPERATION));
            verifyNoMoreInteractions(coordinatorConnector);
        }
    }

    @Test
    public void testPingWorkerThread_shouldStopAfterInterruptedException() {
        Response response = new Response(1L, ALL_WORKERS);
        response.addResponse(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), ResponseType.SUCCESS);

        when(coordinatorConnector.write(eq(ALL_WORKERS), any(PingOperation.class)))
                .thenThrow(new SimulatorProtocolException("expected exception", new InterruptedException()))
                .thenReturn(response);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, 50,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.startWorkerPingThread();

        sleepMillis(300);

        remoteClient.stopWorkerPingThread();

        verify(coordinatorConnector).write(eq(ALL_WORKERS), any(PingOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testPingWorkerThread_shouldContinueAfterOtherException() {
        Response response = new Response(1L, ALL_WORKERS);
        response.addResponse(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), ResponseType.SUCCESS);

        when(coordinatorConnector.write(eq(ALL_WORKERS), any(PingOperation.class)))
                .thenThrow(new SimulatorProtocolException("expected exception", new TimeoutException()))
                .thenReturn(response);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, 50,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.startWorkerPingThread();

        sleepMillis(300);

        remoteClient.stopWorkerPingThread();

        verify(coordinatorConnector, atLeast(2)).write(eq(ALL_WORKERS), any(PingOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testPingWorkerThread_shouldDoNothingIfDisabled() {
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, -1,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
        remoteClient.startWorkerPingThread();

        sleepMillis(300);

        remoteClient.stopWorkerPingThread();

        verifyNoMoreInteractions(coordinatorConnector);
    }

    private void initMockForCreateWorkerOperation(ResponseType responseType) {
        if (responseType != null) {
            Response response = mock(Response.class);
            when(response.getFirstErrorResponseType()).thenReturn(responseType);

            when(coordinatorConnector.write(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenReturn(response);
        } else {
            Exception exception = new SimulatorProtocolException("expected exception");
            when(coordinatorConnector.write(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenThrow(exception);
        }
    }

    private void initMock(ResponseType responseType) {
        Map<SimulatorAddress, ResponseType> responseTypes = new HashMap<SimulatorAddress, ResponseType>();
        responseTypes.put(COORDINATOR, responseType);

        Response response = mock(Response.class);
        when(response.entrySet()).thenReturn(responseTypes.entrySet());

        when(coordinatorConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);
    }

    private ClusterLayout getClusterLayout(int dedicatedMemberMachineCount, int memberWorkerCount, int clientWorkerCount) {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(dedicatedMemberMachineCount);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(memberWorkerCount);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(clientWorkerCount);

        return new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);
    }
}
