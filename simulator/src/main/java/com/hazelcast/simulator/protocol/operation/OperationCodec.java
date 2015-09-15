package com.hazelcast.simulator.protocol.operation;

import com.google.gson.Gson;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;

/**
 * Encodes and decodes a {@link SimulatorOperation}.
 */
public final class OperationCodec {

    private static final Gson GSON = new Gson();

    private OperationCodec() {
    }

    public static String toJson(SimulatorOperation operation) {
        return GSON.toJson(operation);
    }

    public static SimulatorOperation fromJson(String json, Class<? extends SimulatorOperation> classType) {
        return GSON.fromJson(json, classType);
    }

    public static SimulatorOperation fromSimulatorMessage(SimulatorMessage message) {
        return fromJson(message.getOperationData(), message.getOperationType().getClassType());
    }
}
