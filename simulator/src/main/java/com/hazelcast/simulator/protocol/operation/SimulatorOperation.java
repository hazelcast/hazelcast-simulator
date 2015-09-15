package com.hazelcast.simulator.protocol.operation;

/**
 * Marker interface for all Simulator operations, which are the serialized payload of a
 * {@link com.hazelcast.simulator.protocol.core.SimulatorMessage}.
 *
 * Is processed by {@link OperationCodec} with a given {@link com.hazelcast.simulator.protocol.processors.OperationProcessor}.
 */
public interface SimulatorOperation {
}
