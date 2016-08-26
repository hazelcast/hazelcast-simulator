package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.common.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.coordinator.DeploymentPlan.AgentWorkerMode.MIXED;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static com.hazelcast.simulator.common.WorkerType.JAVA_CLIENT;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentPlanTest {
    private final Map<WorkerType, WorkerParameters> workerParametersMap = new HashMap<WorkerType, WorkerParameters>();
    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private SimulatorAddress firstAgent;
    private SimulatorAddress secondAgent;
    private SimulatorAddress thirdAgent;

    @Before
    public void setUp() {
        workerParametersMap.put(WorkerType.MEMBER, mock(WorkerParameters.class));
        workerParametersMap.put(JAVA_CLIENT, mock(WorkerParameters.class));

        firstAgent = componentRegistry.addAgent("192.168.0.1", "192.168.0.1").getAddress();
        secondAgent = componentRegistry.addAgent("192.168.0.2", "192.168.0.2").getAddress();
        thirdAgent = componentRegistry.addAgent("192.168.0.3", "192.168.0.3").getAddress();

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");
    }


    @Test
    public void testGenerateFromArguments_dedicatedMemberCountEqualsAgentCount() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 1, 0, 3);

        assertWorkerDeployment(plan, firstAgent, 1, 0);
        assertWorkerDeployment(plan, secondAgent, 0, 0);
        assertWorkerDeployment(plan, thirdAgent, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noAgents() {
        createDeploymentPlan(new ComponentRegistry(), workerParametersMap, JAVA_CLIENT, 0, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_dedicatedMemberCountNegative() {
        createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 0, -1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_dedicatedMemberCountHigherThanAgentCount() {
        createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 1, 0, 5);
    }

    @Test
    public void testGenerateFromArguments_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 1, 2);

        assertWorkerDeployment(plan, firstAgent, 0, 0);
        assertWorkerDeployment(plan, secondAgent, 0, 0);
        assertWorkerDeployment(plan, thirdAgent, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 1, 3);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noWorkersDefined() {
        createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_singleMemberWorker() {
        //  when(workerParameters.getPerformanceMonitorIntervalSeconds()).thenReturn(10);

        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 1, 0, 0);

        assertWorkerDeployment(plan, firstAgent, 1, 0);
        assertWorkerDeployment(plan, secondAgent, 0, 0);
        assertWorkerDeployment(plan, thirdAgent, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_memberWorkerOverflow() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 4, 0, 0);

        assertWorkerDeployment(plan, firstAgent, 2, 0);
        assertWorkerDeployment(plan, secondAgent, 1, 0);
        assertWorkerDeployment(plan, thirdAgent, 1, 0);
    }

    @Test
    public void testGenerateFromArguments_singleClientWorker() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 1, 0);

        assertWorkerDeployment(plan, firstAgent, 0, 1);
        assertWorkerDeployment(plan, secondAgent, 0, 0);
        assertWorkerDeployment(plan, thirdAgent, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_clientWorkerOverflow() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 5, 0);

        assertWorkerDeployment(plan, firstAgent, 0, 2);
        assertWorkerDeployment(plan, secondAgent, 0, 2);
        assertWorkerDeployment(plan, thirdAgent, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers1() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 2, 3, 1);

        assertWorkerDeployment(plan, firstAgent, 2, 0);
        assertWorkerDeployment(plan, secondAgent, 0, 2);
        assertWorkerDeployment(plan, thirdAgent, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers2() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 2, 3, 2);

        assertWorkerDeployment(plan, firstAgent, 1, 0);
        assertWorkerDeployment(plan, secondAgent, 1, 0);
        assertWorkerDeployment(plan, thirdAgent, 0, 3);
    }

    @Test
    public void testGenerateFromArguments_withIncrementalDeployment_addFirstClientWorkerToLeastCrowdedAgent() {
        WorkerProcessSettings firstWorker = new WorkerProcessSettings(
                1,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );
        WorkerProcessSettings secondWorker = new WorkerProcessSettings(
                1,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );

        componentRegistry.addWorkers(firstAgent, singletonList(firstWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(secondWorker));

        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 4, 0);

        assertWorkerDeployment(plan, firstAgent, 0, 1);
        assertWorkerDeployment(plan, secondAgent, 0, 1);
        assertWorkerDeployment(plan, thirdAgent, 0, 2);
    }

    @Test
    public void testGenerateFromArguments_withIncrementalDeployment_withDedicatedMembers_addClientsToCorrectAgents() {
        WorkerProcessSettings firstWorker = new WorkerProcessSettings(
                1,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>());
        WorkerProcessSettings secondWorker = new WorkerProcessSettings(
                1,
                JAVA_CLIENT,
                "any version",
                "any script",
                0,
                new HashMap<String, String>());
        WorkerProcessSettings thirdWorker = new WorkerProcessSettings(
                2,
                JAVA_CLIENT,
                "any version",
                "any script",
                0,
                new HashMap<String, String>());

        componentRegistry.addWorkers(firstAgent, singletonList(firstWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(secondWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(thirdWorker));
        componentRegistry.addWorkers(thirdAgent, singletonList(secondWorker));
        componentRegistry.addWorkers(thirdAgent, singletonList(thirdWorker));

        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 3, 1);

        assertWorkerDeployment(plan, firstAgent, 0, 0);
        assertWorkerDeployment(plan, secondAgent, 0, 2);
        assertWorkerDeployment(plan, thirdAgent, 0, 1);
    }

    private void assertWorkerDeployment(DeploymentPlan plan, SimulatorAddress agentAddress, int memberCount, int clientCount) {
        List<WorkerProcessSettings> settingsList = plan.getWorkerDeployment().get(agentAddress);
        assertNotNull("Could not find WorkerProcessSettings at index " + agentAddress
                + ", workerDeployment: " + plan.getWorkerDeployment(), settingsList);

        int actualMemberWorkerCount = 0;
        int actualClientWorkerCount = 0;
        for (WorkerProcessSettings workerProcessSettings : settingsList) {
            if (workerProcessSettings.getWorkerType().isMember()) {
                actualMemberWorkerCount++;
            } else {
                actualClientWorkerCount++;
            }
        }
        String prefix = String.format("Agent %s members: %d clients: %d",
                agentAddress,
                actualMemberWorkerCount,
                actualClientWorkerCount);

        assertEquals(prefix + " (memberWorkerCount)", memberCount, actualMemberWorkerCount);
        assertEquals(prefix + " (clientWorkerCount)", clientCount, actualClientWorkerCount);
    }

    @Test
    public void testGetVersionSpecs() {
        AgentData agentData = new AgentData(1, "127.0.0.1", "127.0.0.1");

        testGetVersionSpecs(singletonList(agentData), 1, 0);
    }

    @Test
    public void testGetVersionSpecs_noWorkersOnSecondAgent() {
        AgentData agentData1 = new AgentData(1, "172.16.16.1", "127.0.0.1");
        AgentData agentData2 = new AgentData(2, "172.16.16.2", "127.0.0.1");

        List<AgentData> agents = new ArrayList<AgentData>(2);
        agents.add(agentData1);
        agents.add(agentData2);

        testGetVersionSpecs(agents, 1, 0);
    }

    private void testGetVersionSpecs(List<AgentData> agents, int memberCount, int clientCount) {
        ComponentRegistry componentRegistry = new ComponentRegistry();
        for (AgentData agent : agents) {
            componentRegistry.addAgent(agent.getPublicAddress(), agent.getPrivateAddress());
        }

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getVersionSpec()).thenReturn("outofthebox");

        Map<WorkerType, WorkerParameters> workerParametersMap = new HashMap<WorkerType, WorkerParameters>();
        workerParametersMap.put(WorkerType.MEMBER, workerParameters);

        DeploymentPlan deploymentPlan = createDeploymentPlan(componentRegistry, workerParametersMap,
                JAVA_CLIENT, memberCount, clientCount, 0);

        assertEquals(singleton("outofthebox"), deploymentPlan.getVersionSpecs());
    }
}
