package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvm;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmLauncher;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.InitTestSuiteOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.WorkerType;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.protocol.configuration.Ports.WORKER_START_PORT;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Agent.
 */
public class AgentOperationProcessor extends OperationProcessor {

    private final Agent agent;
    private final ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs;

    public AgentOperationProcessor(ExceptionLogger exceptionLogger, Agent agent,
                                   ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs) {
        super(exceptionLogger);
        this.agent = agent;
        this.workerJVMs = workerJVMs;
    }

    public ConcurrentMap<SimulatorAddress, WorkerJvm> getWorkerJVMs() {
        return workerJVMs;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception {
        switch (operationType) {
            case CREATE_WORKER:
                return processCreateWorker((CreateWorkerOperation) operation);
            case INIT_TEST_SUITE:
                processInitTestSuite((InitTestSuiteOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private ResponseType processCreateWorker(CreateWorkerOperation operation) throws Exception {
        ArrayList<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (WorkerJvmSettings workerJvmSettings : operation.getWorkerJvmSettings()) {
            WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJVMs, workerJvmSettings);
            Future<Boolean> future = getExecutorService().submit(new LaunchWorkerCallable(launcher, workerJvmSettings));
            futures.add(future);
        }
        for (Future<Boolean> future : futures) {
            if (!future.get()) {
                return ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
            }
        }
        return SUCCESS;
    }

    private void processInitTestSuite(InitTestSuiteOperation operation) {
        agent.setTestSuite(operation.getTestSuite());

        File testSuiteDir = new File(Agent.WORKERS_HOME, operation.getTestSuite().getId());
        ensureExistingDirectory(testSuiteDir);

        File libDir = new File(testSuiteDir, "lib");
        ensureExistingDirectory(libDir);
    }

    private class LaunchWorkerCallable implements Callable<Boolean> {

        private final WorkerJvmLauncher launcher;
        private final WorkerJvmSettings workerJvmSettings;

        public LaunchWorkerCallable(WorkerJvmLauncher launcher, WorkerJvmSettings workerJvmSettings) {
            this.launcher = launcher;
            this.workerJvmSettings = workerJvmSettings;
        }

        @Override
        public Boolean call() {
            try {
                launcher.launch();

                int workerIndex = workerJvmSettings.getWorkerIndex();
                int workerPort = WORKER_START_PORT + workerIndex;
                SimulatorAddress workerAddress = agent.getAgentConnector().addWorker(workerIndex, "127.0.0.1", workerPort);

                WorkerType workerType = workerJvmSettings.getWorkerType();
                agent.getCoordinatorLogger().debug(format("Created %s worker %s", workerType, workerAddress));

                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
