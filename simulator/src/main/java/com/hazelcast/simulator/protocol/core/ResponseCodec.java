/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import io.netty.buffer.ByteBuf;

import java.util.Map;

import static com.hazelcast.simulator.protocol.core.BaseCodec.ADDRESS_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.LONG_SIZE;
import static com.hazelcast.simulator.protocol.core.SimulatorAddressCodec.decodeSimulatorAddress;

/**
 * Encodes and decodes a {@link Response}.
 */
public final class ResponseCodec {

    private static final int MAGIC_BYTES = 0x3E5D0B5E;

    private static final int OFFSET_MAGIC_BYTES = INT_SIZE;
    private static final int OFFSET_MESSAGE_ID = OFFSET_MAGIC_BYTES + INT_SIZE;
    private static final int OFFSET_DST_ADDRESS = OFFSET_MESSAGE_ID + LONG_SIZE;

    private static final int HEADER_SIZE = INT_SIZE + LONG_SIZE + ADDRESS_SIZE;
    private static final int DATA_ENTRY_SIZE = ADDRESS_SIZE + INT_SIZE;

    private ResponseCodec() {
    }

    public static void encodeByteBuf(Response response, ByteBuf buffer) {
        buffer.writeInt(HEADER_SIZE + response.size() * DATA_ENTRY_SIZE);
        buffer.writeInt(MAGIC_BYTES);

        buffer.writeLong(response.getMessageId());
        SimulatorAddressCodec.encodeByteBuf(response.getDestination(), buffer);

        for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
            SimulatorAddressCodec.encodeByteBuf(entry.getKey(), buffer);
            buffer.writeInt(entry.getValue().toInt());
        }
    }

    public static Response decodeResponse(ByteBuf buffer) {
        int frameLength = buffer.readInt();
        int dataLength = (frameLength - HEADER_SIZE) / DATA_ENTRY_SIZE;

        if (buffer.readInt() != MAGIC_BYTES) {
            throw new IllegalArgumentException("Invalid magic bytes for Response");
        }

        long messageId = buffer.readLong();
        SimulatorAddress destination = decodeSimulatorAddress(buffer);
        Response response = new Response(messageId, destination);

        for (int i = 0; i < dataLength; i++) {
            SimulatorAddress source = decodeSimulatorAddress(buffer);
            ResponseType responseType = ResponseType.fromInt(buffer.readInt());
            response.addResponse(source, responseType);
        }

        return response;
    }

    public static boolean isResponse(ByteBuf in) {
        return (in.getInt(OFFSET_MAGIC_BYTES) == MAGIC_BYTES);
    }

    public static long getMessageId(ByteBuf in) {
        return in.getLong(OFFSET_MESSAGE_ID);
    }

    public static int getDestinationAddressLevel(ByteBuf in) {
        return in.getInt(OFFSET_DST_ADDRESS);
    }

    public static int getChildAddressIndex(ByteBuf in, int addressLevelValue) {
        return in.getInt(OFFSET_DST_ADDRESS + ((addressLevelValue + 1) * INT_SIZE));
    }
}
