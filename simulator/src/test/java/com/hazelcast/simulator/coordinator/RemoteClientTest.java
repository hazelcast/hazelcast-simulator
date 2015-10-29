package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.cluster.AgentWorkerLayout;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.cluster.ClusterUtils.initMemberLayout;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteClientTest {

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
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.logOnAllAgents("test");

        verify(coordinatorConnector).write(eq(ALL_AGENTS), any(LogOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testLogOnAllWorkers() {
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.logOnAllWorkers("test");

        verify(coordinatorConnector).write(eq(ALL_WORKERS), any(LogOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testCreateWorkers_withClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);

        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(6);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(3);

        List<AgentWorkerLayout> memberLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts, false);
    }

    @Test
    public void testCreateWorkers_noClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);

        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(6);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        List<AgentWorkerLayout> memberLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts, false);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() {
        initMockForCreateWorkerOperation(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);

        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(6);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        List<AgentWorkerLayout> memberLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts, false);
    }

    @Test(expected = SimulatorProtocolException.class)
    public void testCreateWorkers_withExceptionOnWrite() {
        initMockForCreateWorkerOperation(null);

        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(6);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        List<AgentWorkerLayout> memberLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts, false);
    }

    @Test
    public void testCreateWorkersAndTerminateWorkers_withPokeThread() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);

        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(6);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(0);

        List<AgentWorkerLayout> memberLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts, true);

        sleepSeconds(1);

        remoteClient.terminateWorkers(true);
    }

    @Test
    public void testSendToAllAgents() {
        initMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);

        SimulatorOperation operation = new IntegrationTestOperation("test");
        remoteClient.sendToAllAgents(operation);

        verify(coordinatorConnector).write(eq(ALL_AGENTS), eq(operation));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToAllAgents_withErrorResponse() {
        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);

        SimulatorOperation operation = new IntegrationTestOperation("test");
        try {
            remoteClient.sendToAllAgents(operation);
        } finally {
            verify(coordinatorConnector).write(eq(ALL_AGENTS), eq(operation));
            verifyNoMoreInteractions(coordinatorConnector);
        }
    }

    @Test
    public void testSendToAllWorkers() {
        initMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);

        SimulatorOperation operation = new IntegrationTestOperation("test");
        remoteClient.sendToAllWorkers(operation);

        verify(coordinatorConnector).write(eq(ALL_WORKERS), eq(operation));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToAllWorkers_withErrorResponse() {
        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);

        SimulatorOperation operation = new IntegrationTestOperation("test");
        try {
            remoteClient.sendToAllWorkers(operation);
        } finally {
            verify(coordinatorConnector).write(eq(ALL_WORKERS), eq(operation));
            verifyNoMoreInteractions(coordinatorConnector);
        }
    }

    @Test
    public void testSendToFirstWorker() {
        initMock(ResponseType.SUCCESS);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();

        SimulatorOperation operation = new IntegrationTestOperation("test");
        remoteClient.sendToFirstWorker(operation);

        verify(coordinatorConnector).write(eq(firstWorkerAddress), eq(operation));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToFirstWorker_withErrorResponse() {
        initMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();

        SimulatorOperation operation = new IntegrationTestOperation("test");
        try {
            remoteClient.sendToFirstWorker(operation);
        } finally {
            verify(coordinatorConnector).write(eq(firstWorkerAddress), eq(operation));
            verifyNoMoreInteractions(coordinatorConnector);
        }
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
}
