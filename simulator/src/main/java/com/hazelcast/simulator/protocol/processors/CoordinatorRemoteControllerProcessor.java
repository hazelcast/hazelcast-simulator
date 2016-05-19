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

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.WorkerData;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

/**
 * An operation processor to process {@link RemoteControllerOperation} instances
 * from a Simulator Remote Controller on the Simulator Coordinator.
 */
public final class CoordinatorRemoteControllerProcessor implements OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorRemoteControllerProcessor.class);

    private final CoordinatorConnector connector;
    private final ComponentRegistry componentRegistry;

    public CoordinatorRemoteControllerProcessor(CoordinatorConnector connector, ComponentRegistry componentRegistry) {
        this.connector = connector;
        this.componentRegistry = componentRegistry;
    }

    @Override
    public ResponseType process(SimulatorOperation operation, SimulatorAddress sourceAddress) {
        return null;
    }

    public void process(RemoteControllerOperation.Type type) {
        switch (type) {
            case LIST_COMPONENTS:
                processListComponents();
                break;
            default:
                LOGGER.info("This is a NOOP for integration tests");
        }
    }

    private void processListComponents() {
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

        sendResponseToRemoteController(sb.toString().substring(0, sb.length() - 1));
    }

    private void sendResponseToRemoteController(String response) {
        RemoteControllerOperation operation = new RemoteControllerOperation(RemoteControllerOperation.Type.RESPONSE, response);
        connector.writeToRemoteController(operation);
    }
}
