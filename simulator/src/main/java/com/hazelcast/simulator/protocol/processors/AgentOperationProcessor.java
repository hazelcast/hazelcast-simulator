package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvm;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmLauncher;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Agent.
 */
public class AgentOperationProcessor extends OperationProcessor {

    private final ConcurrentMap<String, WorkerJvm> workerJVMs;
    private final Agent agent;

    public AgentOperationProcessor(ExceptionLogger exceptionLogger, Agent agent, ConcurrentMap<String, WorkerJvm> workerJVMs) {
        super(exceptionLogger);
        this.agent = agent;
        this.workerJVMs = workerJVMs;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception {
        switch (operationType) {
            case CREATE_WORKER:
                processCreateWorker((CreateWorkerOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processCreateWorker(CreateWorkerOperation operation) throws Exception {
        WorkerJvmLauncher launcher = new WorkerJvmLauncher(agent, workerJVMs, operation.getWorkerJvmSettings());
        launcher.launch();
    }
}
