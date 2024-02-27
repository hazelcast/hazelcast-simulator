package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.drivers.Driver;
import com.hazelcast.simulator.fake.FakeDriver;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DeploymentPlanTest {
    private final Registry registry = new Registry();
    private SimulatorAddress agent1;
    private SimulatorAddress agent2;
    private SimulatorAddress agent3;
    private Driver<?> driver;

    @Before
    public void before() {
        agent1 = registry.addAgent("192.168.0.1", "192.168.0.1").getAddress();
        agent2 = registry.addAgent("192.168.0.2", "192.168.0.2").getAddress();
        agent3 = registry.addAgent("192.168.0.3", "192.168.0.3").getAddress();
    }

    @Test
    public void dedicatedMemberCountEqualsAgentCount() {
        registry.assignDedicatedMemberMachines(3);
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(1, "member");

        assertDeploymentPlanWorkerCount(plan, agent1, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void whenNoAgents() {
        new DeploymentPlan(new Registry());
    }

    @Test
    public void whenAgentCountSufficientForDedicatedMembersAndClientWorkers() {
        registry.assignDedicatedMemberMachines(2);
        DeploymentPlan plan = new DeploymentPlan(registry.getAgents())
                .addToPlan(1, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 1);
    }


    @Test(expected = CommandLineExitException.class)
    public void whenAgentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        registry.assignDedicatedMemberMachines(3);
        new DeploymentPlan(registry).addToPlan(1, "javaclient");
    }

    @Test
    public void whenSingleMemberWorker() {
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(1, "member");

        assertDeploymentPlanWorkerCount(plan, agent1, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 0);
    }

    @Test
    public void whenMemberWorkerOverflow() {
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(4, "member");

        assertDeploymentPlanWorkerCount(plan, agent1, 2, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 1, 0);
    }

    @Test
    public void whenSingleClientWorker() {
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(1, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 0, 1);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 0);
    }

    @Test
    public void whenClientWorkerOverflow() {
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(5, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 0, 2);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 2);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 1);
    }

    @Test
    public void whenDedicatedAndMixedWorkers1() {
        registry.assignDedicatedMemberMachines(1);
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(2, "member")
                .addToPlan(3, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 2, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 0, 2);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 1);
    }

    @Test
    public void whenDedicatedAndMixedWorkers2() {
        registry.assignDedicatedMemberMachines(2);
        DeploymentPlan plan = new DeploymentPlan(registry)
                .addToPlan(2, "member")
                .addToPlan(3, "javaclient");

        assertDeploymentPlanWorkerCount(plan, agent1, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent2, 1, 0);
        assertDeploymentPlanWorkerCount(plan, agent3, 0, 3);
    }

    @Test
    public void whenIncrementalDeployment_addFirstClientWorkerToLeastCrowdedAgent() {
        DeploymentPlan plan1 = new DeploymentPlan(registry)
                .addToPlan(2, "member");
        for (List<WorkerParameters> workersForAgent : plan1.getWorkerDeployment().values()) {
            registry.addWorkers(workersForAgent);
        }

        DeploymentPlan plan2 = new DeploymentPlan(registry)
                .addToPlan(4, "javaclient");

        assertDeploymentPlanWorkerCount(plan2, agent1, 0, 1);
        assertDeploymentPlanWorkerCount(plan2, agent2, 0, 1);
        assertDeploymentPlanWorkerCount(plan2, agent3, 0, 2);

        assertDeploymentPlanSizePerAgent(plan2, agent1, 1);
        assertDeploymentPlanSizePerAgent(plan2, agent2, 1);
        assertDeploymentPlanSizePerAgent(plan2, agent3, 2);
    }

    @Test
    public void whenIncrementalDeployment_withDedicatedMembers_addClientsToCorrectAgents() {
        registry.assignDedicatedMemberMachines(1);
        DeploymentPlan plan1 = new DeploymentPlan(registry)
                .addToPlan(2, "member");
        for (List<WorkerParameters> workersForAgent : plan1.getWorkerDeployment().values()) {
            registry.addWorkers(workersForAgent);
        }

        DeploymentPlan plan2 = new DeploymentPlan(registry)
                .addToPlan(4, "javaclient");

        assertDeploymentPlanWorkerCount(plan2, agent1, 0, 0);
        assertDeploymentPlanWorkerCount(plan2, agent2, 0, 2);
        assertDeploymentPlanWorkerCount(plan2, agent3, 0, 2);

        assertDeploymentPlanSizePerAgent(plan2, agent1, 0);
        assertDeploymentPlanSizePerAgent(plan2, agent2, 2);
        assertDeploymentPlanSizePerAgent(plan2, agent3, 2);
    }

    private void assertDeploymentPlanWorkerCount(DeploymentPlan plan, SimulatorAddress agentAddress,
                                                 int memberCount, int clientCount) {
        List<WorkerParameters> settingsList = plan.getWorkerDeployment().get(agentAddress);
        assertNotNull(format("Could not find WorkerParameters for Agent %s , workerDeployment: %s",
                agentAddress, plan.getWorkerDeployment()), settingsList);

        int actualMemberWorkerCount = 0;
        int actualClientWorkerCount = 0;
        for (WorkerParameters workerProcessSettings : settingsList) {
            if (workerProcessSettings.getWorkerType().equals("member")) {
                actualMemberWorkerCount++;
            } else {
                actualClientWorkerCount++;
            }
        }
        String prefix = format("Agent %s members: %d clients: %d",
                agentAddress,
                actualMemberWorkerCount,
                actualClientWorkerCount);

        assertEquals(prefix + " (memberWorkerCount)", memberCount, actualMemberWorkerCount);
        assertEquals(prefix + " (clientWorkerCount)", clientCount, actualClientWorkerCount);
    }

    private static void assertDeploymentPlanSizePerAgent(DeploymentPlan plan, SimulatorAddress agentAddress, int expectedSize) {
        Map<SimulatorAddress, List<WorkerParameters>> workerDeployment = plan.getWorkerDeployment();
        List<WorkerParameters> settingsList = workerDeployment.get(agentAddress);
        assertEquals(expectedSize, settingsList.size());
    }


    @Test
    public void testGetVersionSpecs() {
        AgentData agent = new AgentData(1, "127.0.0.1", "127.0.0.1");

        testGetVersionSpecs(singletonList(agent), 1, 0);
    }

    @Test
    public void testGetVersionSpecs_noWorkersOnSecondAgent() {
        AgentData agent1 = new AgentData(1, "172.16.16.1", "127.0.0.1");
        AgentData agent2 = new AgentData(2, "172.16.16.2", "127.0.0.1");

        testGetVersionSpecs(asList(agent1,agent2), 1, 0);
    }

    private void testGetVersionSpecs(List<AgentData> agents, int memberCount, int clientCount) {
        Registry registry = new Registry();
        for (AgentData agent : agents) {
            registry.addAgent(agent.getPublicAddress(), agent.getPrivateAddress());
        }

        driver.set("VERSION_SPEC","outofthebox");
        DeploymentPlan deploymentPlan = new DeploymentPlan(registry)
                .addToPlan(memberCount, "member")
                .addToPlan(clientCount, "javaclient");

        assertEquals(singleton("outofthebox"), deploymentPlan.getVersionSpecs());
    }
}
