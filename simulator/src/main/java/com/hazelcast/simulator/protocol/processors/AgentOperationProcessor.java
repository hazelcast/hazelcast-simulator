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
import com.hazelcast.simulator.utils.EmptyStatement;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.configuration.Ports.WORKER_START_PORT;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Agent.
 */
public class AgentOperationProcessor extends OperationProcessor {

    private static final int EXECUTOR_SERVICE_THREAD_POOL_SIZE = 5;
    private static final int EXECUTOR_SERVICE_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(AgentOperationProcessor.class);

    private final Agent agent;
    private final ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs;
    private final ExecutorService executorService;

    public AgentOperationProcessor(ExceptionLogger exceptionLogger, Agent agent,
                                   ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs) {
        this(exceptionLogger, agent, workerJVMs, Executors.newFixedThreadPool(EXECUTOR_SERVICE_THREAD_POOL_SIZE));
    }

    public AgentOperationProcessor(ExceptionLogger exceptionLogger, Agent agent,
                                   ConcurrentMap<SimulatorAddress, WorkerJvm> workerJVMs, ExecutorService executorService) {
        super(exceptionLogger);
        this.agent = agent;
        this.workerJVMs = workerJVMs;
        this.executorService = executorService;
    }

    public ConcurrentMap<SimulatorAddress, WorkerJvm> getWorkerJVMs() {
        return workerJVMs;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            LOGGER.info("Shutdown of ExecutorService in OperationProcessor...");
            executorService.shutdown();
            executorService.awaitTermination(EXECUTOR_SERVICE_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            LOGGER.info("Shutdown of ExecutorService in OperationProcessor completed!");
        } catch (InterruptedException e) {
            EmptyStatement.ignore(e);
        }
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
