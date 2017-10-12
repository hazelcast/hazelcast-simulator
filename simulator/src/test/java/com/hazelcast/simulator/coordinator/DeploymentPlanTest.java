/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.common.WorkerType.JAVA_CLIENT;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertNotNull;
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
    public void before() {
        workerParametersMap.put(WorkerType.MEMBER, new WorkerParameters());
        workerParametersMap.put(JAVA_CLIENT, new WorkerParameters());

        firstAgent = componentRegistry.addAgent("192.168.0.1", "192.168.0.1").getAddress();
        secondAgent = componentRegistry.addAgent("192.168.0.2", "192.168.0.2").getAddress();
        thirdAgent = componentRegistry.addAgent("192.168.0.3", "192.168.0.3").getAddress();

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");
    }

    @Test
    public void testGenerateFromArguments_dedicatedMemberCountEqualsAgentCount() {
        componentRegistry.assignDedicatedMemberMachines(3);
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 1, 0);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 1, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 0);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noAgents() {
        componentRegistry.assignDedicatedMemberMachines(0);
        createDeploymentPlan(new ComponentRegistry(), workerParametersMap, JAVA_CLIENT, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        componentRegistry.assignDedicatedMemberMachines(2);
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 1);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 0, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 0);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        componentRegistry.assignDedicatedMemberMachines(3);
        createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGenerateFromArguments_noWorkersDefined() {
        createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_singleMemberWorker() {
        //  when(workerParameters.getPerformanceMonitorIntervalSeconds()).thenReturn(10);

        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 1, 0);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 1, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 0);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_memberWorkerOverflow() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 4, 0);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 2, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 1, 0);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 1, 0);
    }

    @Test
    public void testGenerateFromArguments_singleClientWorker() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 1);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 0, 1);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 0);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 0);
    }

    @Test
    public void testGenerateFromArguments_clientWorkerOverflow() {
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 5);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 0, 2);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 2);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers1() {
        componentRegistry.assignDedicatedMemberMachines(1);
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 2, 3);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 2, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 2);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 1);
    }

    @Test
    public void testGenerateFromArguments_dedicatedAndMixedWorkers2() {
        componentRegistry.assignDedicatedMemberMachines(2);
        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 2, 3);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 1, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 1, 0);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 3);
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
                5,
                WorkerType.MEMBER,
                "any version",
                "any script",
                0,
                new HashMap<String, String>()
        );

        componentRegistry.addWorkers(firstAgent, singletonList(firstWorker));
        componentRegistry.addWorkers(secondAgent, singletonList(secondWorker));

        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 4);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 0, 1);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 1);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 2);

        assertDeploymentPlanSizePerAgent(plan, firstAgent, 1);
        assertDeploymentPlanSizePerAgent(plan, secondAgent, 1);
        assertDeploymentPlanSizePerAgent(plan, thirdAgent, 2);

        assertDeploymentPlanWorkerSettings(plan, firstAgent, 0, 2, JAVA_CLIENT);
        assertDeploymentPlanWorkerSettings(plan, secondAgent, 0, 6, JAVA_CLIENT);
        assertDeploymentPlanWorkerSettings(plan, thirdAgent, 0, 1, JAVA_CLIENT);
        assertDeploymentPlanWorkerSettings(plan, thirdAgent, 1, 2, JAVA_CLIENT);
    }

    @Test
    public void testGenerateFromArguments_withIncrementalDeployment_withDedicatedMembers_addClientsToCorrectAgents() {
        componentRegistry.assignDedicatedMemberMachines(1);

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

        DeploymentPlan plan = createDeploymentPlan(componentRegistry, workerParametersMap, JAVA_CLIENT, 0, 3);

        assertDeploymentPlanWorkerCount(plan, firstAgent, 0, 0);
        assertDeploymentPlanWorkerCount(plan, secondAgent, 0, 2);
        assertDeploymentPlanWorkerCount(plan, thirdAgent, 0, 1);
    }

    private void assertDeploymentPlanWorkerCount(DeploymentPlan plan, SimulatorAddress agentAddress,
                                                 int memberCount, int clientCount) {
        List<WorkerProcessSettings> settingsList = plan.getWorkerDeployment().get(agentAddress);
        assertNotNull(format("Could not find WorkerProcessSettings for Agent %s , workerDeployment: %s",
                agentAddress, plan.getWorkerDeployment()), settingsList);

        int actualMemberWorkerCount = 0;
        int actualClientWorkerCount = 0;
        for (WorkerProcessSettings workerProcessSettings : settingsList) {
            if (workerProcessSettings.getWorkerType().isMember()) {
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
        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment = plan.getWorkerDeployment();
        List<WorkerProcessSettings> settingsList = workerDeployment.get(agentAddress);
        assertEquals(expectedSize, settingsList.size());
    }

    private static void assertDeploymentPlanWorkerSettings(DeploymentPlan plan, SimulatorAddress agentAddress, int index,
                                                           int expectedWorkerIndex, WorkerType expectedWorkerType) {
        Map<SimulatorAddress, List<WorkerProcessSettings>> workerDeployment = plan.getWorkerDeployment();
        List<WorkerProcessSettings> settingsList = workerDeployment.get(agentAddress);
        WorkerProcessSettings settings = settingsList.get(index);
        assertEquals(format("Agent: %s, Index: %d, expectedWorkerIndex: %d", agentAddress, index, expectedWorkerIndex),
                expectedWorkerIndex, settings.getWorkerIndex());
        assertEquals(format("Agent: %s, Index: %d, expectedWorkerType: %s", agentAddress, index, expectedWorkerType),
                expectedWorkerType, settings.getWorkerType());
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

    @SuppressWarnings("SameParameterValue")
    private void testGetVersionSpecs(List<AgentData> agents, int memberCount, int clientCount) {
        ComponentRegistry componentRegistry = new ComponentRegistry();
        for (AgentData agent : agents) {
            componentRegistry.addAgent(agent.getPublicAddress(), agent.getPrivateAddress());
        }

        WorkerParameters workerParameters = new WorkerParameters()
                .setVersionSpec("outofthebox");

        Map<WorkerType, WorkerParameters> workerParametersMap = new HashMap<WorkerType, WorkerParameters>();
        workerParametersMap.put(WorkerType.MEMBER, workerParameters);

        DeploymentPlan deploymentPlan = createDeploymentPlan(componentRegistry, workerParametersMap,
                JAVA_CLIENT, memberCount, clientCount);

        assertEquals(singleton("outofthebox"), deploymentPlan.getVersionSpecs());
    }
}
