package com.hazelcast.simulator.protocol.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Response which is sent back to the sender {@link SimulatorAddress} of a {@link SimulatorMessage}.
 *
 * Returns a {@link ResponseType} per destination {@link SimulatorAddress},
 * e.g. if multiple Simulator components have been addressed by a single {@link SimulatorMessage}.
 */
public class Response {

    public static final Response LAST_RESPONSE = new Response(-1);

    private final long messageId;
    private final Map<SimulatorAddress, ResponseType> responseTypes = new HashMap<SimulatorAddress, ResponseType>();

    public Response(long messageId, SimulatorAddress source, ResponseType responseType) {
        this(messageId);
        responseTypes.put(source, responseType);
    }

    public Response(long messageId) {
        this.messageId = messageId;
    }

    public static boolean isLastResponse(Response response) {
        return (response.messageId == LAST_RESPONSE.messageId);
    }

    public void addResponse(SimulatorAddress address, ResponseType responseType) {
        responseTypes.put(address, responseType);
    }

    public void addResponse(Response response) {
        responseTypes.putAll(response.responseTypes);
    }

    public long getMessageId() {
        return messageId;
    }

    public int size() {
        return responseTypes.size();
    }

    public Set<Map.Entry<SimulatorAddress, ResponseType>> entrySet() {
        return responseTypes.entrySet();
    }
}
