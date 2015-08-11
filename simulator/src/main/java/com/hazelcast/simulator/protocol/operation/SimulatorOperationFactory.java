package com.hazelcast.simulator.protocol.operation;

import com.google.gson.Gson;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;

/**
 * Factory to deserialize a {@link SimulatorOperation} from a {@link SimulatorMessage}.
 */
public final class SimulatorOperationFactory {

    private SimulatorOperationFactory() {
    }

    public static SimulatorOperation fromJson(Gson gson, SimulatorMessage message) {
        // TODO: deserialize different operations by {@link SimulatorMessage#getMessageType()}
        return gson.fromJson(message.getMessageData(), ExampleOperation.class);
    }
}
