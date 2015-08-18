package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Processes {@link SimulatorOperation} instances on a Simulator component.
 */
public interface OperationProcessor {

    ResponseType process(SimulatorOperation operation);
}
