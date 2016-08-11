package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.coordinator.deployment.DeploymentPlan;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.coordinator.deployment.DeploymentPlan.createDeploymentPlan;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartWorkersTaskTest {

    private static final int WORKER_PING_INTERVAL_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    private static final int MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS = 0;

    private final ComponentRegistry componentRegistry = new ComponentRegistry();
    private final CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);
    private final WorkerParameters workerParameters = mock(WorkerParameters.class);

    @Before
    public void setup() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");
    }

    @Test
    public void testCreateWorkers_withClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getClusterLayout(0, 6, 3);

        RemoteClient remoteClient = new RemoteClient(
                coordinatorConnector, componentRegistry,
                WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(deploymentPlan, remoteClient, componentRegistry, 0).run();

        assertComponentRegistry(componentRegistry, 6, 3);
    }

    @Test
    public void testCreateWorkers_noClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(deploymentPlan, remoteClient, componentRegistry, 0).run();

        assertComponentRegistry(componentRegistry, 6, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() {
        initMockForCreateWorkerOperation(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(deploymentPlan, remoteClient, componentRegistry, 0).run();
    }

    @Test(expected = SimulatorProtocolException.class)
    public void testCreateWorkers_withExceptionOnWrite() {
        initMockForCreateWorkerOperation(null);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(deploymentPlan, remoteClient, componentRegistry, 0).run();
    }

    private Map<SimulatorAddress, List<WorkerProcessSettings>> getClusterLayout(int dedicatedMemberMachineCount,
                                                                                int memberWorkerCount,
                                                                                int clientWorkerCount) {
        DeploymentPlan deploymentPlan = createDeploymentPlan(componentRegistry, workerParameters,
                memberWorkerCount, clientWorkerCount, dedicatedMemberMachineCount);

        return deploymentPlan.asMap();
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

    private static void assertComponentRegistry(ComponentRegistry componentRegistry,
                                                int expectedMemberCount,
                                                int expectedClientCount) {
        int actualMemberCount = 0;
        int actualClientCount = 0;
        for (WorkerData workerData : componentRegistry.getWorkers()) {
            if (workerData.isMemberWorker()) {
                actualMemberCount++;
            } else {
                actualClientCount++;
            }
        }
        assertEquals(expectedMemberCount, actualMemberCount);
        assertEquals(expectedClientCount, actualClientCount);
    }
}
