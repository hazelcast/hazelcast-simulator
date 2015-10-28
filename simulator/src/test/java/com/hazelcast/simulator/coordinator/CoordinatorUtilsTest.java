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

import com.hazelcast.simulator.cluster.WorkerConfigurationConverter;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.coordinator.AgentWorkerMode.CLIENT;
import static com.hazelcast.simulator.coordinator.AgentWorkerMode.CUSTOM;
import static com.hazelcast.simulator.coordinator.AgentWorkerMode.MEMBER;
import static com.hazelcast.simulator.coordinator.AgentWorkerMode.MIXED;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.initMemberLayout;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.logFailureInfo;
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

    private final ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);
    private final WorkerParameters workerParameters = mock(WorkerParameters.class);
    private final ComponentRegistry componentRegistry = new ComponentRegistry();

    private List<AgentWorkerLayout> agentWorkerLayouts;

    @Before
    public void setUp() {
        componentRegistry.addAgent("192.168.0.1", "192.168.0.1");
        componentRegistry.addAgent("192.168.0.2", "192.168.0.2");
        componentRegistry.addAgent("192.168.0.3", "192.168.0.3");

        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);

        SimulatorProperties simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");

        WorkerConfigurationConverter converter = new WorkerConfigurationConverter(5701, "defaultLicenseKey", workerParameters,
                simulatorProperties, componentRegistry);

        when(clusterLayoutParameters.getWorkerConfigurationConverter()).thenReturn(converter);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CoordinatorUtils.class);
    }

    @Test
    public void testInitMemberLayout_fromXml() {
        String xml = "<clusterConfiguration>\n"
                + "  <workerConfiguration name=\"memberWorker\" type=\"MEMBER\"/>\n"
                + "  <workerConfiguration name=\"clientWorker\" type=\"CLIENT\"/>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"memberWorker\" count=\"1\"/>\n"
                + "  </nodeConfiguration>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"clientWorker\" count=\"2\"/>\n"
                + "  </nodeConfiguration>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"memberWorker\" count=\"3\"/>\n"
                + "    <workerGroup configuration=\"clientWorker\" count=\"4\"/>\n"
                + "  </nodeConfiguration>\n"
                + "</clusterConfiguration>";

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 0);
        assertAgentWorkerLayout(0, CUSTOM, 1, 0);
        assertAgentWorkerLayout(1, CUSTOM, 0, 2);
        assertAgentWorkerLayout(2, CUSTOM, 3, 4);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_fromXml_countMismatch() {
        String xml = "<clusterConfiguration>\n"
                + "  <workerConfiguration name=\"memberWorker\" type=\"MEMBER\"/>\n"
                + "  <workerConfiguration name=\"clientWorker\" type=\"CLIENT\"/>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"memberWorker\" count=\"1\"/>\n"
                + "  </nodeConfiguration>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"clientWorker\" count=\"2\"/>\n"
                + "  </nodeConfiguration>\n"
                + "</clusterConfiguration>";

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_fromXml_hzConfigFileNotExists() {
        String xml = "<clusterConfiguration>\n"
                + "  <workerConfiguration name=\"memberWorker\" type=\"MEMBER\" hzConfigFile=\"notExists\"/>\n"
                + "  <nodeConfiguration>\n"
                + "    <workerGroup configuration=\"memberWorker\" count=\"1\"/>\n"
                + "  </nodeConfiguration>\n"
                + "</clusterConfiguration>";

        when(clusterLayoutParameters.getClusterConfiguration()).thenReturn(xml);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 0);
    }

    @Test
    public void testInitMemberLayout_dedicatedMemberCountEqualsAgentCount() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(3);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 0);
        assertAgentWorkerLayout(0, MEMBER, 0, 0);
        assertAgentWorkerLayout(1, MEMBER, 0, 0);
        assertAgentWorkerLayout(2, MEMBER, 0, 0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_dedicatedMemberCountHigherThanAgentCount() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(5);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 0);
    }

    @Test
    public void testInitMemberLayout_agentCountSufficientForDedicatedMembersAndClientWorkers() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(2);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 1);
        assertAgentWorkerLayout(0, MEMBER, 0, 0);
        assertAgentWorkerLayout(1, MEMBER, 0, 0);
        assertAgentWorkerLayout(2, CLIENT, 0, 1);
    }

    @Test(expected = CommandLineExitException.class)
    public void testInitMemberLayout_agentCountNotSufficientForDedicatedMembersAndClientWorkers() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(3);

        initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 1);
    }

    @Test
    public void testInitMemberLayout_singleMemberWorker() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);
        when(workerParameters.isMonitorPerformance()).thenReturn(true);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 1, 0);
        assertAgentWorkerLayout(0, MIXED, 1, 0);
        assertAgentWorkerLayout(1, MIXED, 0, 0);
        assertAgentWorkerLayout(2, MIXED, 0, 0);
    }

    @Test
    public void testInitMemberLayout_memberWorkerOverflow() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 4, 0);
        assertAgentWorkerLayout(0, MIXED, 2, 0);
        assertAgentWorkerLayout(1, MIXED, 1, 0);
        assertAgentWorkerLayout(2, MIXED, 1, 0);
    }

    @Test
    public void testInitMemberLayout_singleClientWorker() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 1);
        assertAgentWorkerLayout(0, MIXED, 0, 1);
        assertAgentWorkerLayout(1, MIXED, 0, 0);
        assertAgentWorkerLayout(2, MIXED, 0, 0);
    }

    @Test
    public void testClientWorkerOverflow() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(0);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 0, 5);
        assertAgentWorkerLayout(0, MIXED, 0, 2);
        assertAgentWorkerLayout(1, MIXED, 0, 2);
        assertAgentWorkerLayout(2, MIXED, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers1() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(1);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 2, 3);
        assertAgentWorkerLayout(0, MEMBER, 2, 0);
        assertAgentWorkerLayout(1, CLIENT, 0, 2);
        assertAgentWorkerLayout(2, CLIENT, 0, 1);
    }

    @Test
    public void testInitMemberLayout_dedicatedAndMixedWorkers2() {
        when(clusterLayoutParameters.getDedicatedMemberMachineCount()).thenReturn(2);

        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters, 2, 3);
        assertAgentWorkerLayout(0, MEMBER, 1, 0);
        assertAgentWorkerLayout(1, MEMBER, 1, 0);
        assertAgentWorkerLayout(2, CLIENT, 0, 3);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown() {
        final ConcurrentHashMap<SimulatorAddress, Boolean> finishedWorkers = new ConcurrentHashMap<SimulatorAddress, Boolean>();
        finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), true);

        ThreadSpawner spawner = new ThreadSpawner("testWaitForFinishedWorker", true);
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0), true);
                sleepSeconds(1);
                finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 3, 0), true);
            }
        });

        boolean success = waitForWorkerShutdown(3, finishedWorkers.keySet(), CoordinatorUtils.FINISHED_WORKER_TIMEOUT_SECONDS);
        assertTrue(success);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown_withTimeout() {
        ConcurrentHashMap<SimulatorAddress, Boolean> finishedWorkers = new ConcurrentHashMap<SimulatorAddress, Boolean>();
        finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), true);

        boolean success = waitForWorkerShutdown(3, finishedWorkers.keySet(), 1);
        assertFalse(success);
    }

    @Test
    public void testLogFailureInfo_noFailures() {
        logFailureInfo(0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testLogFailureInfo_withFailures() {
        logFailureInfo(1);
    }

    private void assertAgentWorkerLayout(int index, AgentWorkerMode mode, int memberCount, int clientCount) {
        AgentWorkerLayout layout = agentWorkerLayouts.get(index);
        assertNotNull("Could not find AgentWorkerLayout at index " + index, layout);

        String prefix = String.format("Agent %s members: %d clients: %d mode: %s",
                layout.getPublicAddress(),
                layout.getCount(WorkerType.MEMBER),
                layout.getCount(WorkerType.CLIENT),
                layout.getAgentWorkerMode());

        assertEquals(prefix + " (agentWorkerMode)", mode, layout.getAgentWorkerMode());
        assertEquals(prefix + " (memberWorkerCount)", memberCount, layout.getCount(WorkerType.MEMBER));
        assertEquals(prefix + " (clientWorkerCount)", clientCount, layout.getCount(WorkerType.CLIENT));
    }
}
