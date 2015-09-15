package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.initMemberLayout;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NewProtocolAgentsClientTest {

    private CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);

    private CoordinatorParameters parameters = mock(CoordinatorParameters.class);
    private ComponentRegistry registry = new ComponentRegistry();

    @Test
    public void testCreateWorkers_withClients() throws Exception {
        initMocks(ResponseType.SUCCESS, 6, 3);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(registry, parameters);

        NewProtocolAgentsClient agentsClient = new NewProtocolAgentsClient(coordinatorConnector);
        agentsClient.createWorkers(memberLayouts);
    }

    @Test
    public void testCreateWorkers_noClients() throws Exception {
        initMocks(ResponseType.SUCCESS, 6, 0);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(registry, parameters);

        NewProtocolAgentsClient agentsClient = new NewProtocolAgentsClient(coordinatorConnector);
        agentsClient.createWorkers(memberLayouts);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() throws Exception {
        initMocks(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, 6, 0);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(registry, parameters);

        NewProtocolAgentsClient agentsClient = new NewProtocolAgentsClient(coordinatorConnector);
        agentsClient.createWorkers(memberLayouts);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withExceptionOnWrite() throws Exception {
        initMocks(null, 6, 0);

        List<AgentMemberLayout> memberLayouts = initMemberLayout(registry, parameters);

        NewProtocolAgentsClient agentsClient = new NewProtocolAgentsClient(coordinatorConnector);
        agentsClient.createWorkers(memberLayouts);
    }

    private void initMocks(ResponseType responseType, int memberCount, int clientCount) throws Exception {
        if (responseType == null) {
            when(coordinatorConnector.write(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenThrow(
                    new RuntimeException("expected exception"));
        } else {
            Response response = mock(Response.class);
            when(response.getFirstErrorResponseType()).thenReturn(responseType);

            when(coordinatorConnector.write(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenReturn(response);
        }

        registry.addAgent("192.168.0.1", "192.168.0.1");
        registry.addAgent("192.168.0.2", "192.168.0.2");
        registry.addAgent("192.168.0.3", "192.168.0.3");

        when(parameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(parameters.getMemberWorkerCount()).thenReturn(memberCount);
        when(parameters.getClientWorkerCount()).thenReturn(clientCount);
        when(parameters.getProfiler()).thenReturn(JavaProfiler.NONE);
    }
}
