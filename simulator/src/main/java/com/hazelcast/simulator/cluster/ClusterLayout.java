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
package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.coordinator.ClusterLayoutParameters;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.simulator.cluster.ClusterUtils.initMemberLayout;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.formatIpAddress;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.padLeft;
import static java.lang.String.format;

public class ClusterLayout {

    private static final int WORKER_MODE_LENGTH = 6;

    private static final Logger LOGGER = Logger.getLogger(ClusterLayout.class);

    private final Set<String> versionSpecs = new HashSet<String>();

    private final List<AgentWorkerLayout> agentWorkerLayouts;

    private int memberWorkerCount;
    private int clientWorkerCount;

    public ClusterLayout(ComponentRegistry componentRegistry, WorkerParameters workerParameters,
                         ClusterLayoutParameters clusterLayoutParameters) {
        agentWorkerLayouts = initMemberLayout(componentRegistry, workerParameters, clusterLayoutParameters);

        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("Cluster layout");
        LOGGER.info(HORIZONTAL_RULER);

        String layoutType = (clusterLayoutParameters.getClusterConfiguration() == null) ? "arguments" : "cluster.xml";
        LOGGER.info(format("Created via %s:", layoutType));
        for (AgentWorkerLayout agentWorkerLayout : agentWorkerLayouts) {
            Set<String> agentHazelcastVersionSpecs = agentWorkerLayout.getHazelcastVersionSpecs();
            int agentMemberWorkerCount = agentWorkerLayout.getCount(WorkerType.MEMBER);
            int agentClientWorkerCount = agentWorkerLayout.getCount(WorkerType.CLIENT);
            int totalWorkerCount = agentMemberWorkerCount + agentClientWorkerCount;

            versionSpecs.addAll(agentHazelcastVersionSpecs);

            memberWorkerCount += agentMemberWorkerCount;
            clientWorkerCount += agentClientWorkerCount;

            String message = "    Agent %s (%s) members: %s, clients: %s";
            if (totalWorkerCount > 0) {
                message += ", mode: %s, version specs: %s";
            } else {
                message += " (no workers)";
            }
            LOGGER.info(format(message,
                    formatIpAddress(agentWorkerLayout.getPublicAddress()),
                    agentWorkerLayout.getSimulatorAddress(),
                    formatLong(agentMemberWorkerCount, 2),
                    formatLong(agentClientWorkerCount, 2),
                    padLeft(agentWorkerLayout.getAgentWorkerMode().toString(), WORKER_MODE_LENGTH),
                    agentHazelcastVersionSpecs
            ));
        }
    }

    private ClusterLayout() {
        this.agentWorkerLayouts = new ArrayList<AgentWorkerLayout>();
    }

    public static ClusterLayout createSingleInstanceClusterLayout(String agentIpAddress, WorkerParameters workerParameters) {
        AgentData agentData = new AgentData(1, agentIpAddress, agentIpAddress);
        AgentWorkerLayout agentWorkerLayout = new AgentWorkerLayout(agentData, AgentWorkerMode.MEMBER);
        agentWorkerLayout.addWorker(WorkerType.MEMBER, workerParameters);

        ClusterLayout clusterLayout = new ClusterLayout();
        clusterLayout.versionSpecs.addAll(agentWorkerLayout.getHazelcastVersionSpecs());
        clusterLayout.agentWorkerLayouts.add(agentWorkerLayout);
        clusterLayout.memberWorkerCount += agentWorkerLayout.getCount(WorkerType.MEMBER);
        clusterLayout.clientWorkerCount += agentWorkerLayout.getCount(WorkerType.CLIENT);

        return clusterLayout;
    }

    public Set<String> getVersionSpecs() {
        return versionSpecs;
    }

    public List<AgentWorkerLayout> getAgentWorkerLayouts() {
        return agentWorkerLayouts;
    }

    public int getMemberWorkerCount() {
        return memberWorkerCount;
    }

    public int getClientWorkerCount() {
        return clientWorkerCount;
    }

    public int getTotalMemberCount() {
        return memberWorkerCount + clientWorkerCount;
    }
}
