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

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcess;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessLauncher;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.BashOperation;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitSessionOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.FATAL;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Agent.
 */
public class AgentOperationProcessor extends AbstractOperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(AgentOperationProcessor.class);

    private final Agent agent;
    private final WorkerProcessManager workerProcessManager;
    private final ScheduledExecutorService executorService;

    public AgentOperationProcessor(Agent agent,
                                   WorkerProcessManager workerProcessManager,
                                   ScheduledExecutorService executorService) {
        this.agent = agent;
        this.workerProcessManager = workerProcessManager;
        this.executorService = executorService;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                            SimulatorAddress sourceAddress) throws Exception {
        switch (operationType) {
            case INTEGRATION_TEST:
                return processIntegrationTest((IntegrationTestOperation) operation, sourceAddress);
            case INIT_SESSION:
                agent.setSessionId(((InitSessionOperation) operation).getSessionId());
                break;
            case CREATE_WORKER:
                return processCreateWorker((CreateWorkerOperation) operation);
            case START_TIMEOUT_DETECTION:
                processStartTimeoutDetection();
                break;
            case STOP_TIMEOUT_DETECTION:
                processStopTimeoutDetection();
                break;
            case BASH:
                processBashOperation((BashOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processBashOperation(final BashOperation operation) {
        ThreadSpawner spawner = new ThreadSpawner("bash[" + operation.getCommand() + "]");
        for (final WorkerProcess workerProcess : workerProcessManager.getWorkerProcesses()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    Map<String, Object> environment = new HashMap<String, Object>();
                    File pidFile = new File(workerProcess.getWorkerHome(), "worker.pid");
                    if (pidFile.exists()) {
                        environment.put("PID", fileAsText(pidFile));
                    }
                    new BashCommand(operation.getCommand())
                            .setDirectory(workerProcess.getWorkerHome())
                            .addEnvironment(environment)
                            .execute();
                }
            });
        }
    }

    private ResponseType processIntegrationTest(IntegrationTestOperation operation, SimulatorAddress sourceAddress)
            throws Exception {
        SimulatorOperation nestedOperation;
        Response response;
        ResponseFuture future;
        switch (operation.getType()) {
            case NESTED_SYNC:
                nestedOperation = new LogOperation("Sync nested integration test message");
                response = agent.getAgentConnector().write(sourceAddress, nestedOperation);
                LOGGER.debug("Got response for sync nested message: " + response);
                return response.getFirstErrorResponseType();
            case NESTED_ASYNC:
                nestedOperation = new LogOperation("Async nested integration test message");
                future = agent.getAgentConnector().submit(sourceAddress, nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async nested message: " + response);
                return response.getFirstErrorResponseType();
            case DEEP_NESTED_SYNC:
                nestedOperation = new LogOperation("Sync deep nested integration test message");
                response = agent.getAgentConnector().write(COORDINATOR, nestedOperation);
                LOGGER.debug("Got response for sync deep nested message: " + response);
                return response.getFirstErrorResponseType();
            case DEEP_NESTED_ASYNC:
                nestedOperation = new LogOperation("Sync deep nested integration test message");
                future = agent.getAgentConnector().submit(COORDINATOR, nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async deep nested message: " + response);
                return response.getFirstErrorResponseType();
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }

    private ResponseType processCreateWorker(CreateWorkerOperation operation) throws Exception {
        ArrayList<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (WorkerProcessSettings workerProcessSettings : operation.getWorkerProcessSettings()) {
            WorkerProcessLauncher launcher = new WorkerProcessLauncher(agent, workerProcessManager, workerProcessSettings);
            LaunchWorkerCallable task = new LaunchWorkerCallable(launcher, workerProcessSettings);
            Future<Boolean> future = executorService.schedule(task, operation.getDelayMs(), TimeUnit.MILLISECONDS);
            futures.add(future);
        }
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                LOGGER.error("Failed to start Worker, settings response type EXCEPTION_DURING_OPERATION_EXECUTION...");
                return ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
            }
        }
        return SUCCESS;
    }

    private void processStartTimeoutDetection() {
        agent.getWorkerProcessFailureMonitor().startTimeoutDetection();
    }

    private void processStopTimeoutDetection() {
        agent.getWorkerProcessFailureMonitor().stopTimeoutDetection();
    }

    private final class LaunchWorkerCallable implements Callable<Boolean> {

        private final WorkerProcessLauncher launcher;
        private final WorkerProcessSettings workerProcessSettings;

        private LaunchWorkerCallable(WorkerProcessLauncher launcher, WorkerProcessSettings workerProcessSettings) {
            this.launcher = launcher;
            this.workerProcessSettings = workerProcessSettings;
        }

        @Override
        public Boolean call() {
            try {
                launcher.launch();

                int workerIndex = workerProcessSettings.getWorkerIndex();
                int workerPort = agent.getPort() + workerIndex;
                SimulatorAddress workerAddress = agent.getAgentConnector().addWorker(workerIndex, "127.0.0.1", workerPort);

                WorkerType workerType = workerProcessSettings.getWorkerType();

                LogOperation logOperation = new LogOperation(format("Created %s Worker %s", workerType, workerAddress), DEBUG);
                agent.getAgentConnector().submit(COORDINATOR, logOperation);

                return true;
            } catch (Exception e) {
                LOGGER.error("Failed to start Worker", e);

                LogOperation logOperation = new LogOperation("Failed to start Worker: " + e.getMessage(), FATAL);
                agent.getAgentConnector().submit(COORDINATOR, logOperation);

                return false;
            }
        }
    }
}
