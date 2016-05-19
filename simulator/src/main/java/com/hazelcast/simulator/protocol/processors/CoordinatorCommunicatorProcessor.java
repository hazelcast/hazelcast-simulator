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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

public final class CoordinatorCommunicatorProcessor {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorCommunicatorProcessor.class);

    private final ServerConnector connector;
    private final ComponentRegistry componentRegistry;

    public CoordinatorCommunicatorProcessor(ServerConnector connector, ComponentRegistry componentRegistry) {
        this.connector = connector;
        this.componentRegistry = componentRegistry;
    }

    public void process(RemoteControllerOperation.Type type) {
        switch (type) {
            case SHOW_COMPONENTS:
                StringBuilder sb = new StringBuilder();
                sb.append("Simulator components:").append(NEW_LINE);

                sb.append("Agents (").append(componentRegistry.agentCount()).append("):").append(NEW_LINE);
                for (AgentData agentData : componentRegistry.getAgents()) {
                    sb.append(" * ")
                            .append(agentData.getAddress()).append(": ")
                            .append(agentData.getPublicAddress()).append(" (")
                            .append(agentData.getPrivateAddress()).append(")")
                            .append(NEW_LINE);
                }

                sb.append("Workers (").append(componentRegistry.workerCount()).append("):").append(NEW_LINE);
                for (WorkerData workerData : componentRegistry.getWorkers()) {
                    sb.append(" * ")
                            .append(workerData.getAddress()).append(": ")
                            .append(workerData.getSettings().getWorkerType())
                            .append(NEW_LINE);
                }

                logToCommunicator(sb.toString().substring(0, sb.length() - 1));
                break;
            default:
                LOGGER.info("This is a NOOP for integration tests");
        }
    }

    private void logToCommunicator(String message) {
        LogOperation operation = new LogOperation(message);
        connector.submit(SimulatorAddress.REMOTE, operation);
    }
}
