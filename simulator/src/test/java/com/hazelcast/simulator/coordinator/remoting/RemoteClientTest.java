package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
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
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.initMemberLayout;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteClientTest {

    private CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);

    private CoordinatorParameters parameters = mock(CoordinatorParameters.class);
    private ComponentRegistry componentRegistry = new ComponentRegistry();

    @Test
    public void testLogOnAllWorkers() {
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.logOnAllAgents("test");

        verify(coordinatorConnector).write(eq(ALL_AGENTS), any(LogOperation.class));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test
    public void testCreateWorkers_withClients() {
        initCreateWorkersMocks(ResponseType.SUCCESS, 6, 3);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(componentRegistry, parameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts);
    }

    @Test
    public void testCreateWorkers_noClients() {
        initCreateWorkersMocks(ResponseType.SUCCESS, 6, 0);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(componentRegistry, parameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() {
        initCreateWorkersMocks(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, 6, 0);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(componentRegistry, parameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts);
    }

    @Test(expected = SimulatorProtocolException.class)
    public void testCreateWorkers_withExceptionOnWrite() {
        initCreateWorkersMocks(null, 6, 0);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(componentRegistry, parameters);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        remoteClient.createWorkers(memberLayouts);
    }

    @Test
    public void testSendToAllAgents() {
        iniCoordinatorConnectorMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);

        SimulatorOperation operation = new IntegrationTestOperation("test");
        remoteClient.sendToAllAgents(operation);

        verify(coordinatorConnector).write(eq(ALL_AGENTS), eq(operation));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToAllAgents_withErrorResponse() {
        iniCoordinatorConnectorMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
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
        iniCoordinatorConnectorMock(ResponseType.SUCCESS);
        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);

        SimulatorOperation operation = new IntegrationTestOperation("test");
        remoteClient.sendToAllWorkers(operation);

        verify(coordinatorConnector).write(eq(ALL_WORKERS), eq(operation));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToAllWorkers_withErrorResponse() {
        iniCoordinatorConnectorMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
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
        iniCoordinatorConnectorMock(ResponseType.SUCCESS);
        initCommonMocks(1, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry);
        SimulatorAddress firstWorkerAddress = componentRegistry.getFirstWorker().getAddress();

        SimulatorOperation operation = new IntegrationTestOperation("test");
        remoteClient.sendToFirstWorker(operation);

        verify(coordinatorConnector).write(eq(firstWorkerAddress), eq(operation));
        verifyNoMoreInteractions(coordinatorConnector);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendToFirstWorker_withErrorResponse() {
        iniCoordinatorConnectorMock(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        initCommonMocks(1, 0);

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

    private void initCreateWorkersMocks(ResponseType responseType, int memberCount, int clientCount) {
        if (responseType != null) {
            Response response = mock(Response.class);
            when(response.getFirstErrorResponseType()).thenReturn(responseType);

            when(coordinatorConnector.write(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenReturn(response);
        } else {
            Exception exception = new SimulatorProtocolException("expected exception");
            when(coordinatorConnector.write(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenThrow(exception);
        }

        initCommonMocks(memberCount, clientCount);
    }

    private void iniCoordinatorConnectorMock(ResponseType responseType) {
        Map<SimulatorAddress, ResponseType> responseTypes = new HashMap<SimulatorAddress, ResponseType>();
        responseTypes.put(COORDINATOR, responseType);

        Response response = mock(Response.class);
        when(response.entrySet()).thenReturn(responseTypes.entrySet());

        when(coordinatorConnector.write(any(SimulatorAddress.class), any(SimulatorOperation.class))).thenReturn(response);
    }

    private void initCommonMocks(int memberCount, int clientCount) {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        WorkerJvmSettings workerJvmSettings = mock(WorkerJvmSettings.class);
        when(workerJvmSettings.getWorkerIndex()).thenReturn(1);

        SimulatorAddress agentAddress = componentRegistry.getFirstAgent().getAddress();
        componentRegistry.addWorkers(agentAddress, Collections.singletonList(workerJvmSettings));

        when(parameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(parameters.getMemberWorkerCount()).thenReturn(memberCount);
        when(parameters.getClientWorkerCount()).thenReturn(clientCount);
        when(parameters.getProfiler()).thenReturn(JavaProfiler.NONE);
    }
}
