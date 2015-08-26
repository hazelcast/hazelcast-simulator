package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;

/**
 * Processes {@link SimulatorOperation} instances on a Simulator component.
 */
public abstract class OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(OperationProcessor.class);

    public final ResponseType process(SimulatorOperation operation) {
        OperationType operationType = operation.getOperationType();
        LOGGER.info(getClass().getSimpleName() + ".process(" + operation.getClass().getSimpleName() + ")");
        try {
            switch (operationType) {
                case INTEGRATION_TEST:
                    if (!IntegrationTestOperation.TEST_DATA.equals(((IntegrationTestOperation) operation).getTestData())) {
                        throw new IllegalStateException();
                    }
                    return SUCCESS;
                default:
                    return processOperation(operation);
            }
        } catch (Exception e) {
            LOGGER.error("Error during processing an operation", e);
            return EXCEPTION_DURING_OPERATION_EXECUTION;
        }
    }

    protected abstract ResponseType processOperation(SimulatorOperation operation) throws Exception;
}
