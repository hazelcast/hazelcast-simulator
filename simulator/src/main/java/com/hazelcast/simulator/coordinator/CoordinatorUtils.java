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

import com.hazelcast.simulator.cluster.ClusterConfiguration;
import com.hazelcast.simulator.cluster.NodeConfiguration;
import com.hazelcast.simulator.cluster.WorkerConfiguration;
import com.hazelcast.simulator.cluster.WorkerGroup;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.cluster.ClusterConfigurationUtils.fromXml;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

final class CoordinatorUtils {

    public static final int FINISHED_WORKER_TIMEOUT_SECONDS = 120;
    public static final int FINISHED_WORKERS_SLEEP_MILLIS = 500;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorUtils.class);

    private CoordinatorUtils() {
    }

    public static List<AgentWorkerLayout> initMemberLayout(ComponentRegistry registry, WorkerParameters parameters,
                                                           ClusterLayoutParameters clusterLayoutParameters,
                                                           int memberWorkerCount, int clientWorkerCount) {
        List<AgentWorkerLayout> agentWorkerLayouts = initAgentWorkerLayouts(registry);

        if (clusterLayoutParameters.getClusterConfiguration() != null) {
            generateFromXml(agentWorkerLayouts, registry.agentCount(), clusterLayoutParameters, parameters);
        } else {
            generateFromArguments(agentWorkerLayouts, clusterLayoutParameters.getDedicatedMemberMachineCount(),
                    registry.agentCount(), memberWorkerCount, clientWorkerCount, parameters);
        }

        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            LOGGER.info(format("    Agent %s members: %d, clients: %d, mode: %s",
                    agentWorkerLayout.getPublicAddress(),
                    agentWorkerLayout.getCount(WorkerType.MEMBER),
                    agentWorkerLayout.getCount(WorkerType.CLIENT),
                    agentWorkerLayout.getAgentWorkerMode()
            ));
        }

        return agentWorkerLayouts;
    }

    private static List<AgentWorkerLayout> initAgentWorkerLayouts(ComponentRegistry componentRegistry) {
        List<AgentWorkerLayout> agentWorkerLayouts = new LinkedList<AgentWorkerLayout>();
        for (AgentData agentData : componentRegistry.getAgents()) {
            AgentWorkerLayout layout = new AgentWorkerLayout(agentData, AgentWorkerMode.MIXED);
            agentWorkerLayouts.add(layout);
        }
        return agentWorkerLayouts;
    }

    private static void generateFromXml(List<AgentWorkerLayout> agentWorkerLayouts, int agentCount,
                                        ClusterLayoutParameters clusterLayoutParameters, WorkerParameters parameters) {
        ClusterConfiguration clusterConfiguration = getClusterConfiguration(clusterLayoutParameters);
        if (clusterConfiguration.size() != agentCount) {
            throw new CommandLineExitException(format("Found %d node configurations for %d agents (number must be equal)",
                    clusterConfiguration.size(), agentCount));
        }

        Iterator<AgentWorkerLayout> iterator = agentWorkerLayouts.iterator();
        for (NodeConfiguration nodeConfiguration : clusterConfiguration.getNodeConfigurations()) {
            AgentWorkerLayout agentWorkerLayout = iterator.next();
            for (WorkerGroup workerGroup : nodeConfiguration.getWorkerGroups()) {
                WorkerConfiguration workerConfig = clusterConfiguration.getWorkerConfiguration(workerGroup.getConfiguration());
                for (int i = 0; i < workerGroup.getCount(); i++) {
                    agentWorkerLayout.addWorker(workerConfig.getType(), parameters, workerConfig);
                }
            }
            agentWorkerLayout.setAgentWorkerMode(AgentWorkerMode.CUSTOM);
        }
    }

    private static ClusterConfiguration getClusterConfiguration(ClusterLayoutParameters clusterLayoutParameters) {
        try {
            return fromXml(clusterLayoutParameters);
        } catch (Exception e) {
            throw new CommandLineExitException("Could not parse cluster configuration", e);
        }
    }

    private static void generateFromArguments(List<AgentWorkerLayout> agentWorkerLayouts, int dedicatedMemberMachineCount,
                                              int agentCount, int memberWorkerCount, int clientWorkerCount,
                                              WorkerParameters parameters) {
        if (dedicatedMemberMachineCount > agentCount) {
            throw new CommandLineExitException(format("dedicatedMemberMachineCount %d can't be larger than number of agents %d",
                    dedicatedMemberMachineCount, agentCount));
        }
        if (clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            throw new CommandLineExitException("dedicatedMemberMachineCount is too big, there are no machines left for clients!");
        }

        assignDedicatedMemberMachines(agentCount, agentWorkerLayouts, dedicatedMemberMachineCount);

        AtomicInteger currentIndex = new AtomicInteger(0);
        for (int i = 0; i < memberWorkerCount; i++) {
            // assign members
            AgentWorkerLayout agentWorkerLayout = findNextAgentLayout(currentIndex, agentWorkerLayouts, AgentWorkerMode.CLIENT);
            agentWorkerLayout.addWorker(WorkerType.MEMBER, parameters);
        }
        for (int i = 0; i < clientWorkerCount; i++) {
            // assign clients
            AgentWorkerLayout agentWorkerLayout = findNextAgentLayout(currentIndex, agentWorkerLayouts, AgentWorkerMode.MEMBER);
            agentWorkerLayout.addWorker(WorkerType.CLIENT, parameters);
        }
    }

    private static void assignDedicatedMemberMachines(int agentCount, List<AgentWorkerLayout> agentWorkerLayouts,
                                                      int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentWorkerMode(agentWorkerLayouts, 0, dedicatedMemberMachineCount, AgentWorkerMode.MEMBER);
            assignAgentWorkerMode(agentWorkerLayouts, dedicatedMemberMachineCount, agentCount, AgentWorkerMode.CLIENT);
        }
    }

    private static void assignAgentWorkerMode(List<AgentWorkerLayout> agentWorkerLayouts, int startIndex, int endIndex,
                                              AgentWorkerMode agentWorkerMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentWorkerLayouts.get(i).setAgentWorkerMode(agentWorkerMode);
        }
    }

    private static AgentWorkerLayout findNextAgentLayout(AtomicInteger currentIndex, List<AgentWorkerLayout> agentWorkerLayouts,
                                                         AgentWorkerMode excludedAgentWorkerMode) {
        int size = agentWorkerLayouts.size();
        while (true) {
            AgentWorkerLayout agentLayout = agentWorkerLayouts.get(currentIndex.getAndIncrement() % size);
            if (agentLayout.getAgentWorkerMode() != excludedAgentWorkerMode) {
                return agentLayout;
            }
        }
    }

    static ConcurrentMap<TestPhase, CountDownLatch> getTestPhaseSyncMap(boolean isParallel, int testCount,
                                                                        TestPhase latestTestPhaseToSync) {
        if (!isParallel) {
            return null;
        }
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

    static boolean waitForWorkerShutdown(int expectedFinishedWorkerCount, Set<SimulatorAddress> finishedWorkers,
                                         int timeoutSeconds) {
        LOGGER.info(format("Waiting %d seconds for shutdown of %d workers...", timeoutSeconds, expectedFinishedWorkerCount));
        long timeoutTimestamp = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (finishedWorkers.size() < expectedFinishedWorkerCount && System.currentTimeMillis() < timeoutTimestamp) {
            sleepMillis(FINISHED_WORKERS_SLEEP_MILLIS);
        }
        int remainingWorkers = expectedFinishedWorkerCount - finishedWorkers.size();
        if (remainingWorkers > 0) {
            LOGGER.warn(format("Aborted waiting for shutdown of all workers (%d still running)...", remainingWorkers));
            return false;
        }
        LOGGER.info("Shutdown of all workers completed...");
        return true;
    }

    static void logFailureInfo(int failureCount) {
        if (failureCount > 0) {
            LOGGER.fatal(HORIZONTAL_RULER);
            LOGGER.fatal(failureCount + " failures have been detected!!!");
            LOGGER.fatal(HORIZONTAL_RULER);
            throw new CommandLineExitException(failureCount + " failures have been detected");
        }
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("No failures have been detected!");
        LOGGER.info(HORIZONTAL_RULER);
    }
}
