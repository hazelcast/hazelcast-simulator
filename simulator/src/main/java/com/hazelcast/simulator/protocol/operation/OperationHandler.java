package com.hazelcast.simulator.protocol.operation;

import com.google.gson.Gson;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;

/**
 * Processes the serialized {@link SimulatorOperation} of a {@link SimulatorMessage} with the given {@link OperationProcessor}.
 */
public final class OperationHandler {

    private static final Gson GSON = new Gson();

    private OperationHandler() {
    }

    public static String encodeOperation(SimulatorOperation operation) {
        return GSON.toJson(operation);
    }

    public static ResponseType processMessage(SimulatorMessage message, OperationProcessor processor) {
        SimulatorOperation operation = GSON.fromJson(message.getOperationData(), message.getOperationType().getClassType());
        return processor.process(operation);
    }
}
