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

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;

final class CoordinatorUtils {

    public static final int FINISHED_WORKER_TIMEOUT_SECONDS = 60;
    public static final int FINISHED_WORKERS_SLEEP_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorUtils.class);

    private CoordinatorUtils() {
    }

    static String createAddressConfig(String tagName, ComponentRegistry componentRegistry, int port) {
        StringBuilder members = new StringBuilder();
        for (AgentData agentData : componentRegistry.getAgents()) {
            String hostAddress = agentData.getPrivateAddress();
            members.append("<").append(tagName).append(">")
                    .append(hostAddress)
                    .append(":").append(port)
                    .append("</").append(tagName).append(">\n");
        }
        return members.toString();
    }

    static int getPort(String memberHzConfig) {
        ByteArrayInputStream bis = null;
        try {
            byte[] configString = memberHzConfig.getBytes("UTF-8");
            bis = new ByteArrayInputStream(configString);
            Config config = new XmlConfigBuilder(bis).build();

            return config.getNetworkConfig().getPort();
        } catch (Exception e) {
            throw new CommandLineExitException("Could not get port from settings", e);
        } finally {
            closeQuietly(bis);
        }
    }

    public static List<AgentMemberLayout> initMemberLayout(ComponentRegistry registry, WorkerParameters parameters,
                                                           int dedicatedMemberMachineCount,
                                                           int memberWorkerCount, int clientWorkerCount) {
        int agentCount = registry.agentCount();
        if (dedicatedMemberMachineCount > agentCount) {
            throw new CommandLineExitException(format("dedicatedMemberMachineCount %d can't be larger than number of agents %d",
                    dedicatedMemberMachineCount, agentCount));
        }
        if (clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            throw new CommandLineExitException("dedicatedMemberMachineCount is too big, there are no machines left for clients!");
        }

        List<AgentMemberLayout> agentMemberLayouts = initAgentMemberLayouts(registry);

        assignDedicatedMemberMachines(agentCount, agentMemberLayouts, dedicatedMemberMachineCount);

        AtomicInteger currentIndex = new AtomicInteger(0);
        for (int i = 0; i < memberWorkerCount; i++) {
            // assign server nodes
            AgentMemberLayout agentLayout = findNextAgentLayout(currentIndex, agentMemberLayouts, AgentMemberMode.CLIENT);
            agentLayout.addWorker(WorkerType.MEMBER, parameters);
        }
        for (int i = 0; i < clientWorkerCount; i++) {
            // assign the clients
            AgentMemberLayout agentLayout = findNextAgentLayout(currentIndex, agentMemberLayouts, AgentMemberMode.MEMBER);
            agentLayout.addWorker(WorkerType.CLIENT, parameters);
        }

        // log the layout
        for (AgentMemberLayout agentMemberLayout : agentMemberLayouts) {
            LOGGER.info(format("    Agent %s members: %d clients: %d mode: %s",
                    agentMemberLayout.getPublicAddress(),
                    agentMemberLayout.getCount(WorkerType.MEMBER),
                    agentMemberLayout.getCount(WorkerType.CLIENT),
                    agentMemberLayout.getAgentMemberMode()
            ));
        }

        return agentMemberLayouts;
    }

    private static List<AgentMemberLayout> initAgentMemberLayouts(ComponentRegistry componentRegistry) {
        List<AgentMemberLayout> agentMemberLayouts = new LinkedList<AgentMemberLayout>();
        for (AgentData agentData : componentRegistry.getAgents()) {
            AgentMemberLayout layout = new AgentMemberLayout(agentData, AgentMemberMode.MIXED);
            agentMemberLayouts.add(layout);
        }
        return agentMemberLayouts;
    }

    private static void assignDedicatedMemberMachines(int agentCount, List<AgentMemberLayout> agentMemberLayouts,
                                                      int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentMemberMode(agentMemberLayouts, 0, dedicatedMemberMachineCount, AgentMemberMode.MEMBER);
            assignAgentMemberMode(agentMemberLayouts, dedicatedMemberMachineCount, agentCount, AgentMemberMode.CLIENT);
        }
    }

    private static void assignAgentMemberMode(List<AgentMemberLayout> agentMemberLayouts, int startIndex, int endIndex,
                                              AgentMemberMode agentMemberMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentMemberLayouts.get(i).setAgentMemberMode(agentMemberMode);
        }
    }

    private static AgentMemberLayout findNextAgentLayout(AtomicInteger currentIndex, List<AgentMemberLayout> agentMemberLayouts,
                                                         AgentMemberMode excludedAgentMemberMode) {
        int size = agentMemberLayouts.size();
        while (true) {
            AgentMemberLayout agentLayout = agentMemberLayouts.get(currentIndex.getAndIncrement() % size);
            if (agentLayout.getAgentMemberMode() != excludedAgentMemberMode) {
                return agentLayout;
            }
        }
    }

    static ConcurrentMap<TestPhase, CountDownLatch> getTestPhaseSyncMap(TestPhase latestTestPhaseToSync, int testCount) {
        ConcurrentMap<TestPhase, CountDownLatch> testPhaseSyncMap = new ConcurrentHashMap<TestPhase, CountDownLatch>();
        boolean useTestCount = true;
        for (TestPhase testPhase : TestPhase.values()) {
            testPhaseSyncMap.put(testPhase, new CountDownLatch(useTestCount ? testCount : 0));
            if (testPhase == latestTestPhaseToSync) {
                useTestCount = false;
            }
        }
        return testPhaseSyncMap;
    }

    static boolean waitForWorkerShutdown(int expectedFinishedWorkerCount, Set<String> finishedWorkers, int timeoutSeconds) {
        LOGGER.info(format("Waiting %d seconds for shutdown of %d workers...", expectedFinishedWorkerCount, timeoutSeconds));
        long timeoutTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (finishedWorkers.size() < expectedFinishedWorkerCount && System.currentTimeMillis() < timeoutTimestamp) {
            sleepMillis(FINISHED_WORKERS_SLEEP_MILLIS);
        }
        if (finishedWorkers.size() == expectedFinishedWorkerCount) {
            LOGGER.info("Shutdown of all workers completed...");
            return true;
        }
        int remainingWorkers = expectedFinishedWorkerCount - finishedWorkers.size();
        LOGGER.warn(format("Aborted waiting for shutdown of all workers (%d still running)...", remainingWorkers));
        return false;
    }
}
