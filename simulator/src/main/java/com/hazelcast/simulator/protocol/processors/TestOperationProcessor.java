package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Test.
 */
public class TestOperationProcessor extends OperationProcessor {

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception {
        switch (operationType) {
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
    }
}
