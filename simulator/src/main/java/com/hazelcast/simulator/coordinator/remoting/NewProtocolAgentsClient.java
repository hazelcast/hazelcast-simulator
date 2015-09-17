package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.coordinator.AgentMemberLayout;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * This class is an interim solution to handle the parallel existence of the old and new Simulator Communication protocol.
 */
public class NewProtocolAgentsClient {

    private static final Logger LOGGER = Logger.getLogger(NewProtocolAgentsClient.class);

    private final CoordinatorConnector coordinatorConnector;
    private final ComponentRegistry componentRegistry;

    public NewProtocolAgentsClient(CoordinatorConnector coordinatorConnector, ComponentRegistry componentRegistry) {
        this.coordinatorConnector = coordinatorConnector;
        this.componentRegistry = componentRegistry;
    }

    public void createWorkers(List<AgentMemberLayout> agentLayouts) {
        createWorkersByType(agentLayouts, true);
        createWorkersByType(agentLayouts, false);
    }

    private void createWorkersByType(List<AgentMemberLayout> agentLayouts, boolean isMemberType) {
        ThreadSpawner spawner = new ThreadSpawner("createWorkers", true);
        for (AgentMemberLayout agentMemberLayout : agentLayouts) {
            final List<WorkerJvmSettings> settingsList = new ArrayList<WorkerJvmSettings>();
            for (WorkerJvmSettings workerJvmSettings : agentMemberLayout.getWorkerJvmSettings()) {
                WorkerType workerType = workerJvmSettings.getWorkerType();
                if (workerType.isMember() == isMemberType) {
                    settingsList.add(workerJvmSettings);
                }
            }

            final int workerCount = settingsList.size();
            if (workerCount == 0) {
                continue;
            }
            final SimulatorAddress destination = agentMemberLayout.getSimulatorAddress();
            final String workerType = (isMemberType) ? "member" : "client";
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    CreateWorkerOperation operation = new CreateWorkerOperation(settingsList);
                    Response response = coordinatorConnector.write(destination, operation);

                    ResponseType responseType = response.getFirstErrorResponseType();
                    if (responseType != ResponseType.SUCCESS) {
                        throw new CommandLineExitException(format(
                                "Could not create %d %s worker on %s (%s)", workerCount, workerType, destination, responseType));
                    }

                    LOGGER.info(format("Created %d %s worker on %s", workerCount, workerType, destination));
                    componentRegistry.addWorkers(destination, settingsList);
                }
            });
        }
        spawner.awaitCompletion();
    }
}
