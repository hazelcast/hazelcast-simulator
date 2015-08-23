package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;

/**
 * An {@link OperationProcessor} to process {@link SimulatorOperation} instances on a Simulator Coordinator.
 */
public class CoordinatorOperationProcessor implements OperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorOperationProcessor.class);

    @Override
    public ResponseType process(SimulatorOperation operation) {
        LOGGER.info("CoordinatorOperationProcessor.process() " + operation);

        return SUCCESS;
    }
}
