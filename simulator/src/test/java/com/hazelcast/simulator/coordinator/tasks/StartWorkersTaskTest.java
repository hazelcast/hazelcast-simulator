package com.hazelcast.simulator.coordinator.tasks;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.DeploymentPlan;
import com.hazelcast.simulator.coordinator.RemoteClient;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartWorkersTaskTest {

    private static final int WORKER_PING_INTERVAL_MILLIS = (int) SECONDS.toMillis(10);

    private final ComponentRegistry componentRegistry = new ComponentRegistry();
    private final CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);
    private final Map<WorkerType,WorkerParameters> workerParametersMap = new HashMap<WorkerType, WorkerParameters>();
    private RemoteClient remoteClient;

    @Before
    public void before() {
        workerParametersMap.put(WorkerType.MEMBER, new WorkerParameters());
        workerParametersMap.put(WorkerType.JAVA_CLIENT, new WorkerParameters());

        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");
    }

    @After
    public void after() {
        closeQuietly(remoteClient);
    }

    @Test
    public void testCreateWorkers_withClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getDeployment(0, 6, 3);

        remoteClient = new RemoteClient(
                coordinatorConnector, componentRegistry,
                WORKER_PING_INTERVAL_MILLIS);

        new StartWorkersTask(deploymentPlan, Collections.<String, String>emptyMap(), remoteClient, componentRegistry, 0).run();

        assertComponentRegistry(componentRegistry, 6, 3);
    }

    @Test
    public void testCreateWorkers_noClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getDeployment(0, 6, 0);

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS);

        new StartWorkersTask(deploymentPlan, Collections.<String, String>emptyMap(),remoteClient, componentRegistry, 0).run();

        assertComponentRegistry(componentRegistry, 6, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() {
        initMockForCreateWorkerOperation(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getDeployment(0, 6, 0);

        remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS);

        new StartWorkersTask(deploymentPlan, Collections.<String, String>emptyMap(), remoteClient, componentRegistry, 0).run();
    }

    @Test(expected = SimulatorProtocolException.class)
    public void testCreateWorkers_withExceptionOnWrite() {
        try {
            initMockForCreateWorkerOperation(null);
            Map<SimulatorAddress, List<WorkerProcessSettings>> deploymentPlan = getDeployment(0, 6, 0);

            remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS);

            new StartWorkersTask(deploymentPlan, Collections.<String, String>emptyMap(), remoteClient, componentRegistry, 0).run();
        }catch (RuntimeException e){
            e.printStackTrace();
            throw e;
        }
    }

    private Map<SimulatorAddress, List<WorkerProcessSettings>> getDeployment(int dedicatedMemberMachineCount,
                                                                             int memberWorkerCount,
                                                                             int clientWorkerCount) {
        componentRegistry.assignDedicatedMemberMachines(dedicatedMemberMachineCount);
        DeploymentPlan deploymentPlan = createDeploymentPlan(componentRegistry, workerParametersMap,
                WorkerType.JAVA_CLIENT,  memberWorkerCount, clientWorkerCount);

        return deploymentPlan.getWorkerDeployment();
    }

    private void initMockForCreateWorkerOperation(ResponseType responseType) {
        if (responseType != null) {
            Response response = mock(Response.class);
            when(response.getFirstErrorResponseType()).thenReturn(responseType);

            when(coordinatorConnector.invoke(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenReturn(response);
        } else {
            Exception exception = new SimulatorProtocolException("expected exception");
            when(coordinatorConnector.invoke(any(SimulatorAddress.class), any(CreateWorkerOperation.class))).thenThrow(exception);
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
