package com.hazelcast.simulator.protocol.core;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import static com.hazelcast.simulator.protocol.core.BaseCodec.ADDRESS_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.LONG_SIZE;
import static com.hazelcast.simulator.protocol.core.Response.LAST_RESPONSE;
import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

/**
 * Encodes and decodes a {@link Response}.
 */
public final class ResponseCodec {

    private static final int MAGIC_BYTES = 0x3E5D0B5E;

    private static final int OFFSET_MAGIC_BYTES = INT_SIZE;

    private static final int HEADER_SIZE = INT_SIZE + LONG_SIZE;
    private static final int DATA_ENTRY_SIZE = ADDRESS_SIZE + INT_SIZE;

    private ResponseCodec() {
    }

    public static void encodeByteBuf(Response response, ByteBuf buffer) {
        buffer.writeInt(HEADER_SIZE + response.size() * DATA_ENTRY_SIZE);
        buffer.writeInt(MAGIC_BYTES);

        buffer.writeLong(response.getMessageId());

        for (Map.Entry<SimulatorAddress, ResponseType> entry : response.entrySet()) {
            SimulatorAddressCodec.encodeByteBuf(entry.getKey(), buffer);
            buffer.writeInt(entry.getValue().toInt());
        }
    }

    public static Response decodeResponse(ByteBuf buffer) {
        if (EMPTY_BUFFER.equals(buffer)) {
            return LAST_RESPONSE;
        }

        int frameLength = buffer.readInt();
        int dataLength = (frameLength - HEADER_SIZE) / DATA_ENTRY_SIZE;

        if (buffer.readInt() != MAGIC_BYTES) {
            throw new IllegalArgumentException("Invalid magic bytes for Response");
        }

        long messageId = buffer.readLong();
        Response response = new Response(messageId);

        for (int i = 0; i < dataLength; i++) {
            SimulatorAddress source = SimulatorAddressCodec.decodeSimulatorAddress(buffer);
            ResponseType responseType = ResponseType.fromInt(buffer.readInt());
            response.addResponse(source, responseType);
        }

        return response;
    }

    public static boolean isResponse(ByteBuf in) {
        return (in.getInt(OFFSET_MAGIC_BYTES) == MAGIC_BYTES);
    }
}
