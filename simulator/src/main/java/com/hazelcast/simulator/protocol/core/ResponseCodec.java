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

import io.netty.buffer.ByteBuf;

import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.LONG_SIZE;
import static com.hazelcast.simulator.protocol.core.SimulatorAddressCodec.decodeSimulatorAddress;
import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Encodes and decodes a {@link Response}.
 */
public final class ResponseCodec {

    private static final int MAGIC_BYTES = 0x3E5D0B5E;

    private static final int OFFSET_MAGIC_BYTES = INT_SIZE;
    private static final int OFFSET_MESSAGE_ID = OFFSET_MAGIC_BYTES + INT_SIZE;
    private static final int OFFSET_DST_ADDRESS = OFFSET_MESSAGE_ID + LONG_SIZE;

    private ResponseCodec() {
    }

    public static void encodeByteBuf(Response response, ByteBuf buffer) {
        Set<Map.Entry<SimulatorAddress, Response.Part>> parts = response.getParts();

        // write place holder for length. Eventually we'll overwrite it with the correct length.
        buffer.writeInt(0);
        int start = buffer.writerIndex();

        buffer.writeInt(MAGIC_BYTES);
        buffer.writeLong(response.getMessageId());
        SimulatorAddressCodec.encodeByteBuf(response.getDestination(), buffer);

        buffer.writeInt(parts.size());
        for (Map.Entry<SimulatorAddress, Response.Part> entry : parts) {
            SimulatorAddressCodec.encodeByteBuf(entry.getKey(), buffer);
            Response.Part part = entry.getValue();
            buffer.writeInt(part.getType().toInt());

            String payload = part.getPayload();
            if (payload == null) {
                buffer.writeInt(-1);
            } else {
                byte[] data = payload.getBytes(UTF_8);
                buffer.writeInt(data.length);
                buffer.writeBytes(data);
            }
        }

        int length = buffer.writerIndex() - start;
        buffer.setInt(start - INT_SIZE, length);
    }

    public static Response decodeResponse(ByteBuf buffer) {
        int frameLength = buffer.readInt();

        if (buffer.readInt() != MAGIC_BYTES) {
            throw new IllegalArgumentException("Invalid magic bytes for Response");
        }

        long messageId = buffer.readLong();
        SimulatorAddress destination = decodeSimulatorAddress(buffer);
        Response response = new Response(messageId, destination);

        int partCount = buffer.readInt();
        for (int i = 0; i < partCount; i++) {
            SimulatorAddress source = decodeSimulatorAddress(buffer);
            ResponseType responseType = ResponseType.fromInt(buffer.readInt());

            String payload = null;
            int size = buffer.readInt();
            if (size > -1) {
                payload = buffer.readSlice(size).toString(UTF_8);
            }

            response.addPart(source, responseType, payload);
        }

        return response;
    }

    public static boolean isResponse(ByteBuf in) {
        return in.getInt(OFFSET_MAGIC_BYTES) == MAGIC_BYTES;
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
