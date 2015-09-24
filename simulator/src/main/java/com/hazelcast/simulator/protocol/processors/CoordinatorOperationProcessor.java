package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.coordinator.PerformanceStateContainer;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.operation.ExceptionOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.PerformanceStateOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Coordinator.
 */
public class CoordinatorOperationProcessor extends OperationProcessor {

    private final LocalExceptionLogger exceptionLogger;
    private final PerformanceStateContainer performanceStateContainer;

    public CoordinatorOperationProcessor(LocalExceptionLogger exceptionLogger,
                                         PerformanceStateContainer performanceStateContainer) {
        super(exceptionLogger);
        this.exceptionLogger = exceptionLogger;
        this.performanceStateContainer = performanceStateContainer;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception {
        switch (operationType) {
            case EXCEPTION:
                processException((ExceptionOperation) operation);
                break;
            case PERFORMANCE_STATE:
                processPerformanceState((PerformanceStateOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processException(ExceptionOperation operation) {
        exceptionLogger.logOperation(operation);
    }

    private void processPerformanceState(PerformanceStateOperation operation) {
        performanceStateContainer.updatePerformanceState(operation.getWorkerAddress(), operation.getPerformanceStates());
    }
}
