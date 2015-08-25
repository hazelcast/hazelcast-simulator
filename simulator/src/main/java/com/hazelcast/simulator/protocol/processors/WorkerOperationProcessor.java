package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static org.junit.Assert.assertEquals;

/**
 * An {@link OperationProcessor} to process {@link SimulatorOperation} instances on a Simulator Worker.
 */
public class WorkerOperationProcessor implements OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessor.class);

    @Override
    public ResponseType process(SimulatorOperation operation) {
        LOGGER.info("WorkerOperationProcessor.process() " + operation.getClass().getSimpleName());

        switch (operation.getOperationType()) {
            case INTEGRATION_TEST_OPERATION:
                assertEquals(IntegrationTestOperation.TEST_DATA, ((IntegrationTestOperation) operation).getTestData());
                return SUCCESS;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }
}
