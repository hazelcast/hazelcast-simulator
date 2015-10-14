package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.IsPhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkersOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_WORKERS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static java.lang.String.format;

public class RemoteClient {

    private static final int WAIT_FOR_PHASE_COMPLETION_INTERVAL_SECONDS = 5;

    private static final Logger LOGGER = Logger.getLogger(RemoteClient.class);

    private final CoordinatorConnector coordinatorConnector;
    private final ComponentRegistry componentRegistry;

    public RemoteClient(CoordinatorConnector coordinatorConnector, ComponentRegistry componentRegistry) {
        this.coordinatorConnector = coordinatorConnector;
        this.componentRegistry = componentRegistry;
    }

    public void logOnAllAgents(String message) {
        coordinatorConnector.write(ALL_AGENTS, new LogOperation(COORDINATOR, message));
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
            final SimulatorAddress agentAddress = agentMemberLayout.getSimulatorAddress();
            final String workerType = (isMemberType) ? "member" : "client";
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    CreateWorkerOperation operation = new CreateWorkerOperation(settingsList);
                    Response response = coordinatorConnector.write(agentAddress, operation);

                    ResponseType responseType = response.getFirstErrorResponseType();
                    if (responseType != ResponseType.SUCCESS) {
                        throw new CommandLineExitException(format("Could not create %d %s worker on %s (%s)",
                                workerCount, workerType, agentAddress, responseType));
                    }

                    LOGGER.info(format("Created %d %s worker on %s", workerCount, workerType, agentAddress));
                    componentRegistry.addWorkers(agentAddress, settingsList);
                }
            });
        }
        spawner.awaitCompletion();
    }

    public void terminateWorkers() {
        sendToAllWorkers(new TerminateWorkersOperation());
    }

    public void initTestSuite(TestSuite testSuite) {
        sendToAllAgents(new InitTestSuiteOperation(testSuite));
    }

    public void waitForPhaseCompletion(String prefix, String testId, TestPhase testPhase) {
        long start = System.nanoTime();
        IsPhaseCompletedOperation operation = new IsPhaseCompletedOperation(testId, testPhase);
        for (; ; ) {
            Response response = coordinatorConnector.write(ALL_WORKERS, operation);
            boolean complete = true;
            for (Map.Entry<SimulatorAddress, ResponseType> responseTypeEntry : response.entrySet()) {
                ResponseType responseType = responseTypeEntry.getValue();
                if (responseType == ResponseType.TEST_PHASE_IS_RUNNING) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return;
            }
            long duration = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
            LOGGER.info(prefix + "Waiting " + secondsToHuman(duration) + " for " + testPhase.desc() + " completion");
            sleepSeconds(WAIT_FOR_PHASE_COMPLETION_INTERVAL_SECONDS);
        }
    }

    public void sendToAllAgents(SimulatorOperation operation) {
        Response response = coordinatorConnector.write(ALL_AGENTS, operation);
        validateResponse(operation, response);
    }

    public void sendToAllWorkers(SimulatorOperation operation) {
        Response response = coordinatorConnector.write(ALL_WORKERS, operation);
        validateResponse(operation, response);
    }

    public void sendToFirstWorker(SimulatorOperation operation) {
        Response response = coordinatorConnector.write(componentRegistry.getFirstWorker().getAddress(), operation);
        validateResponse(operation, response);
    }

    private void validateResponse(SimulatorOperation operation, Response response) {
        for (Map.Entry<SimulatorAddress, ResponseType> responseTypeEntry : response.entrySet()) {
            ResponseType responseType = responseTypeEntry.getValue();
            if (responseType != ResponseType.SUCCESS) {
                SimulatorAddress source = responseTypeEntry.getKey();
                throw new CommandLineExitException(format("Could not execute %s on %s (%s)",
                        operation, source, responseType));
            }
        }
    }
}
