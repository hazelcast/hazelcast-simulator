package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Coordinator.
 */
public class CoordinatorOperationProcessor extends OperationProcessor {

    @Override
    protected ResponseType processOperation(SimulatorOperation operation) throws Exception {
        switch (operation.getOperationType()) {
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }
}
