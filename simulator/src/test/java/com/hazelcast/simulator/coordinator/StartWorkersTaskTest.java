package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StartWorkersTaskTest {
    private static final int WORKER_PING_INTERVAL_MILLIS = (int) TimeUnit.SECONDS.toMillis(10);
    private final ComponentRegistry componentRegistry = new ComponentRegistry();
    private static final int MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS = 0;

    private final CoordinatorConnector coordinatorConnector = mock(CoordinatorConnector.class);
    private final ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
    private final WorkerParameters workerParameters = mock(WorkerParameters.class);
    private static final String DEFAULT_TEST_ID = "RemoteClientTest";

    @Before
    public void setup() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        WorkerProcessSettings workerProcessSettings = mock(WorkerProcessSettings.class);
        when(workerProcessSettings.getWorkerIndex()).thenReturn(1);

        SimulatorAddress agentAddress = componentRegistry.getFirstAgent().getAddress();
        componentRegistry.addWorkers(agentAddress, Collections.singletonList(workerProcessSettings));

        TestCase testCase = new TestCase(DEFAULT_TEST_ID);

        TestSuite testSuite = new TestSuite("RemoteClientTest");
        testSuite.addTest(testCase);

        componentRegistry.addTests(testSuite);
    }

    // this test has no value since it doesn't verify anything. It just triggers the code to run.
    @Test
    public void testCreateWorkers_withClients() {
        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 3);

        RemoteClient remoteClient= new RemoteClient(
                coordinatorConnector, componentRegistry,
                WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(clusterLayout, remoteClient, componentRegistry, 0).run();
    }

//    @Test
//    public void testCreateWorkers_noClients() {
//        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
//        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);
//
//        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
//                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);
//        remoteClient.createWorkers(clusterLayout, false);
//    }
//
    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkers_withErrorResponse() {
        initMockForCreateWorkerOperation(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(clusterLayout, remoteClient, componentRegistry, 0).run();
    }

    @Test(expected = SimulatorProtocolException.class)
    public void testCreateWorkers_withExceptionOnWrite() {
        initMockForCreateWorkerOperation(null);
        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);

        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS);

        new StartWorkersTask(clusterLayout, remoteClient, componentRegistry, 0).run();
    }

//    @Test
//    public void testCreateWorkersAndTerminateWorkers_withPokeThread() {
//        initMockForCreateWorkerOperation(ResponseType.SUCCESS);
//        ClusterLayout clusterLayout = getClusterLayout(0, 6, 0);
//
//        RemoteClient remoteClient = new RemoteClient(coordinatorConnector, componentRegistry, WORKER_PING_INTERVAL_MILLIS,
//                MEMBER_WORKER_SHUTDOWN_DELAY_SECONDS, 0);
//        remoteClient.createWorkers(clusterLayout, true);
//
//        sleepSeconds(1);
//
//        remoteClient.terminateWorkers(true);
//    }


    private ClusterLayout getClusterLayout(int dedicatedMemberMachineCount, int memberWorkerCount, int clientWorkerCount) {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(dedicatedMemberMachineCount);
        when(clusterLayoutParameters.getMemberWorkerCount()).thenReturn(memberWorkerCount);
        when(clusterLayoutParameters.getClientWorkerCount()).thenReturn(clientWorkerCount);

        return new ClusterLayout(componentRegistry, workerParameters, clusterLayoutParameters);
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
}
