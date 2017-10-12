/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.protocol.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Response which is sent back to the sender {@link SimulatorAddress} of a {@link SimulatorMessage}.
 * <p>
 * Returns a {@link ResponseType} per destination {@link SimulatorAddress},
 * e.g. if multiple Simulator components have been addressed by a single {@link SimulatorMessage}.
 */
public class Response {

    private final Map<SimulatorAddress, Part> parts = new HashMap<SimulatorAddress, Part>();

    private final long messageId;
    private final SimulatorAddress destination;

    public Response(SimulatorMessage message) {
        this(message.getMessageId(), message.getSource());
    }

    public Response(SimulatorMessage message, ResponseType responseType) {
        this(message.getMessageId(), message.getSource(), message.getDestination(), responseType);
    }

    public Response(long messageId, SimulatorAddress destination, SimulatorAddress source, ResponseType responseType) {
        this(messageId, destination);
        parts.put(source, new Part(responseType, null));
    }

    public Response(long messageId, SimulatorAddress destination) {
        this.messageId = messageId;
        this.destination = destination;
    }

    public Response addPart(SimulatorAddress address, ResponseType responseType, String payload) {
        parts.put(address, new Part(responseType, payload));
        return this;
    }

    public Response addPart(SimulatorAddress address, ResponseType responseType) {
        addPart(address, responseType, null);
        return this;
    }

    public void addAllParts(Response response) {
        parts.putAll(response.parts);
    }

    public long getMessageId() {
        return messageId;
    }

    public SimulatorAddress getDestination() {
        return destination;
    }

    public int size() {
        return parts.size();
    }

    public Set<Map.Entry<SimulatorAddress, Part>> getParts() {
        return parts.entrySet();
    }

    public Part getFirstPart() {
        return parts.values().iterator().next();
    }

    public Part getPart(SimulatorAddress address) {
        return parts.get(address);
    }

    public static class Part {
        private final ResponseType type;
        private final String payload;

        public Part(ResponseType type, String payload) {
            this.type = type;
            this.payload = payload;
        }

        public ResponseType getType() {
            return type;
        }

        public String getPayload() {
            return payload;
        }

        @Override
        public String toString() {
            return "Part{type=" + type + ", payload='" + payload + "'}";
        }
    }

    public ResponseType getFirstErrorResponseType() {
        for (Part entry : parts.values()) {
            if (entry.getType() != ResponseType.SUCCESS) {
                return entry.type;
            }
        }
        return ResponseType.SUCCESS;
    }

    public Part getFirstErrorPart() {
        for (Part part : parts.values()) {
            if (part.getType() != ResponseType.SUCCESS) {
                return part;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "Response{"
                + "messageId=" + messageId
                + ", destination=" + destination
                + ", parts=" + parts
                + '}';
    }
}
