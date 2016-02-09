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
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmLauncher;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Agent.
 */
public class AgentOperationProcessor extends OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(AgentOperationProcessor.class);

    private final Agent agent;
    private final WorkerJvmManager workerJvmManager;
    private final ExecutorService executorService;

    public AgentOperationProcessor(ExceptionLogger exceptionLogger, Agent agent, WorkerJvmManager workerJvmManager,
                                   ExecutorService executorService) {
        super(exceptionLogger);
        this.agent = agent;
        this.workerJvmManager = workerJvmManager;
        this.executorService = executorService;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                            SimulatorAddress sourceAddress) throws Exception {
        switch (operationType) {
            case INTEGRATION_TEST:
                return processIntegrationTest((IntegrationTestOperation) operation, sourceAddress);
            case INIT_TEST_SUITE:
                processInitTestSuite((InitTestSuiteOperation) operation);
                break;
            case CREATE_WORKER:
                return processCreateWorker((CreateWorkerOperation) operation);
            case START_TIMEOUT_DETECTION:
                processStartTimeoutDetection();
                break;
            case STOP_TIMEOUT_DETECTION:
                processStopTimeoutDetection();
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
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
                response = agent.getAgentConnector().write(SimulatorAddress.COORDINATOR, nestedOperation);
                LOGGER.debug("Got response for sync deep nested message: " + response);
                return response.getFirstErrorResponseType();
            case DEEP_NESTED_ASYNC:
                nestedOperation = new LogOperation("Sync deep nested integration test message");
                future = agent.getAgentConnector().submit(SimulatorAddress.COORDINATOR, nestedOperation);
                response = future.get();
                LOGGER.debug("Got response for async deep nested message: " + response);
                return response.getFirstErrorResponseType();
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }

    private void processInitTestSuite(InitTestSuiteOperation operation) {
        agent.setTestSuite(operation.getTestSuite());

        File workersHome = ensureExistingDirectory(getSimulatorHome(), "workers");
        File testSuiteDir = ensureExistingDirectory(workersHome, operation.getTestSuite().getId());
        ensureExistingDirectory(testSuiteDir, "lib");
    }

    private ResponseType processCreateWorker(CreateWorkerOperation operation) throws Exception {
        ArrayList<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (WorkerJvmSettings workerJvmSettings : operation.getWorkerJvmSettings()) {
            WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJvmManager, workerJvmSettings);
            Future<Boolean> future = executorService.submit(new LaunchWorkerCallable(launcher, workerJvmSettings));
            futures.add(future);
        }
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                return ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
            }
        }
        return SUCCESS;
    }

    private void processStartTimeoutDetection() {
        agent.getWorkerJvmFailureMonitor().startTimeoutDetection();
    }

    private void processStopTimeoutDetection() {
        agent.getWorkerJvmFailureMonitor().stopTimeoutDetection();
    }

    private final class LaunchWorkerCallable implements Callable<Boolean> {

        private final WorkerJvmLauncher launcher;
        private final WorkerJvmSettings workerJvmSettings;

        private LaunchWorkerCallable(WorkerJvmLauncher launcher, WorkerJvmSettings workerJvmSettings) {
            this.launcher = launcher;
            this.workerJvmSettings = workerJvmSettings;
        }

        @Override
        public Boolean call() {
            try {
                launcher.launch();

                int workerIndex = workerJvmSettings.getWorkerIndex();
                int workerPort = agent.getPort() + workerIndex;
                SimulatorAddress workerAddress = agent.getAgentConnector().addWorker(workerIndex, "127.0.0.1", workerPort);

                WorkerType workerType = workerJvmSettings.getWorkerType();
                agent.getCoordinatorLogger().debug(format("Created %s Worker %s", workerType, workerAddress));

                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
