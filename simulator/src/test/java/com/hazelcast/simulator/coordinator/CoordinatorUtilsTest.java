/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.createAddressConfig;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.getPort;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.initMemberLayout;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.waitForWorkerShutdown;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorUtilsTest {

    private final WorkerParameters workerParameters = mock(WorkerParameters.class);
    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private List<AgentMemberLayout> agentMemberLayouts;

    @Before
    public void setUp() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CoordinatorUtils.class);
    }

    @Test
    public void testGetPort() {
        String memberConfig = FileUtils.fileAsText("./dist/src/main/dist/conf/hazelcast.xml");

        int port = getPort(memberConfig);
        assertEquals(5701, port);
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetPort_withException() {
        getPort("");
    }

    @Test
    public void testCreateAddressConfig() {
        List<AgentData> agents = new ArrayList<AgentData>();
        for (int i = 1; i <= 5; i++) {
            AgentData agentData = mock(AgentData.class);
            when(agentData.getPrivateAddress()).thenReturn("192.168.0." + i);
            agents.add(agentData);
        }

        ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
        when(componentRegistry.getAgents()).thenReturn(agents);

        String addressConfig = createAddressConfig("members", componentRegistry, 6666);
        for (int i = 1; i <= 5; i++) {
            assertTrue(addressConfig.contains("192.168.0." + i + ":6666"));
        }
    }

    @Test
    public void testInitMemberLayout_dedicatedMemberCountEqualsAgentCount() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 3, 0, 0);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MEMBER, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_dedicatedMemberCountHigherThanAgentCount() {
        initMemberLayout(componentRegistry, workerParameters, 5, 0, 0);
    }

    @Test
    public void testInitMemberLayout_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 2, 0, 1);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        initMemberLayout(componentRegistry, workerParameters, 3, 0, 1);
    }

    @Test
    public void testInitMemberLayout_singleMemberWorker() {
        when(workerParameters.isMonitorPerformance()).thenReturn(true);

        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 0, 1, 0);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 1, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 0);
    }

    @Test
    public void testInitMemberLayout_memberWorkerOverflow() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 0, 4, 0);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 2, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 1, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 1, 0);
    }

    @Test
    public void testInitMemberLayout_singleClientWorker() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 0, 0, 1);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 0, 1);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 0);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 0);
    }

    @Test
    public void testClientWorkerOverflow() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 0, 0, 5);
        assertAgentMemberLayout(0, AgentMemberMode.MIXED, 0, 2);
        assertAgentMemberLayout(1, AgentMemberMode.MIXED, 0, 2);
        assertAgentMemberLayout(2, AgentMemberMode.MIXED, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers1() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 1, 2, 3);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 2, 0);
        assertAgentMemberLayout(1, AgentMemberMode.CLIENT, 0, 2);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers2() {
        agentMemberLayouts = initMemberLayout(componentRegistry, workerParameters, 2, 2, 3);
        assertAgentMemberLayout(0, AgentMemberMode.MEMBER, 1, 0);
        assertAgentMemberLayout(1, AgentMemberMode.MEMBER, 1, 0);
        assertAgentMemberLayout(2, AgentMemberMode.CLIENT, 0, 3);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown() {
        final ConcurrentHashMap<String, Boolean> finishedWorkers = new ConcurrentHashMap<String, Boolean>();
        finishedWorkers.put("A", true);

        ThreadSpawner spawner = new ThreadSpawner("testWaitForFinishedWorker", true);
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                finishedWorkers.put("B", true);
                sleepSeconds(1);
                finishedWorkers.put("C", true);
            }
        });

        boolean success = waitForWorkerShutdown(3, finishedWorkers.keySet(), CoordinatorUtils.FINISHED_WORKER_TIMEOUT_SECONDS);
        assertTrue(success);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown_withTimeout() {
        final ConcurrentHashMap<String, Boolean> finishedWorkers = new ConcurrentHashMap<String, Boolean>();
        finishedWorkers.put("A", true);

        boolean success = waitForWorkerShutdown(3, finishedWorkers.keySet(), 1);
        assertFalse(success);
    }

    private void assertAgentMemberLayout(int index, AgentMemberMode mode, int memberCount, int clientCount) {
        AgentMemberLayout layout = agentMemberLayouts.get(index);
        assertNotNull("Could not find AgentMemberLayout at index " + index, layout);

        String prefix = String.format("Agent %s members: %d clients: %d mode: %s",
                layout.getPublicAddress(),
                layout.getCount(WorkerType.MEMBER),
                layout.getCount(WorkerType.CLIENT),
                layout.getAgentMemberMode());

        assertEquals(prefix + " (agentMemberMode)", mode, layout.getAgentMemberMode());
        assertEquals(prefix + " (memberWorkerCount)", memberCount, layout.getCount(WorkerType.MEMBER));
        assertEquals(prefix + " (clientWorkerCount)", clientCount, layout.getCount(WorkerType.CLIENT));
    }
}
