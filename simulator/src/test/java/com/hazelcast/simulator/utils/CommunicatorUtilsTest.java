package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.mockito.Mockito.mock;

public class CommunicatorUtilsTest {

    private ComponentRegistry componentRegistry;

    @Before
    public void setUp() {
        SimulatorAddress agent = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);

        WorkerParameters workerParameters = mock(WorkerParameters.class);

        List<WorkerProcessSettings> settingsList = new ArrayList<WorkerProcessSettings>();
        settingsList.add(new WorkerProcessSettings(1, WorkerType.MEMBER, workerParameters));
        settingsList.add(new WorkerProcessSettings(2, WorkerType.CLIENT, workerParameters));

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");
        componentRegistry.addWorkers(agent, settingsList);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CommunicatorUtils.class);
    }

    @Test
    public void testExecute_withIntegrationTest() {
        CommunicatorUtils.execute(RemoteControllerOperation.Type.INTEGRATION_TEST, componentRegistry);
    }

    @Test
    public void testExecute_withShowComponents() {
        CommunicatorUtils.execute(RemoteControllerOperation.Type.SHOW_COMPONENTS, componentRegistry);
    }
}
