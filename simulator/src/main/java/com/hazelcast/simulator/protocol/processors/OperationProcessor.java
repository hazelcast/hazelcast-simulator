package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static java.lang.String.format;

/**
 * Processes {@link SimulatorOperation} instances on a Simulator component.
 */
public abstract class OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(OperationProcessor.class);

    private final ExceptionLogger exceptionLogger;

    OperationProcessor(ExceptionLogger exceptionLogger) {
        this.exceptionLogger = exceptionLogger;
    }

    public void shutdown() {
    }

    public final ResponseType process(SimulatorOperation operation) {
        OperationType operationType = getOperationType(operation);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getClass().getSimpleName() + ".process(" + operation.getClass().getSimpleName() + ")");
        }
        try {
            switch (operationType) {
                case INTEGRATION_TEST:
                    processIntegrationTest((IntegrationTestOperation) operation);
                    break;
                case LOG:
                    processLog((LogOperation) operation);
                    break;
                default:
                    return processOperation(operationType, operation);
            }
        } catch (Exception e) {
            exceptionLogger.log(e);
            return EXCEPTION_DURING_OPERATION_EXECUTION;
        }
        return SUCCESS;
    }

    private void processIntegrationTest(IntegrationTestOperation operation) {
        if (!IntegrationTestOperation.TEST_DATA.equals(operation.getTestData())) {
            throw new IllegalStateException("operationData has not the expected value");
        }
    }

    private void processLog(LogOperation operation) {
        LOGGER.log(operation.getLevel(), format("[%s] %s", operation.getSource(), operation.getMessage()));
    }

    protected abstract ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception;
}
