package com.hazelcast.simulator.protocol.core;

import io.netty.buffer.ByteBuf;

import static com.hazelcast.simulator.protocol.core.BaseCodec.ADDRESS_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.LONG_SIZE;
import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Encodes and decodes a {@link SimulatorMessage}.
 */
public final class SimulatorMessageCodec {

    private static final int MAGIC_BYTES = 0xA5E1CA57;

    private static final int OFFSET_DST_ADDRESS = 2 * INT_SIZE;
    private static final int OFFSET_MESSAGE_ID = OFFSET_DST_ADDRESS + 2 * ADDRESS_SIZE;

    private static final int HEADER_SIZE = 2 * INT_SIZE + LONG_SIZE + 2 * ADDRESS_SIZE;

    private SimulatorMessageCodec() {
    }

    public static void encodeByteBuf(SimulatorMessage msg, ByteBuf buffer) {
        byte[] data = msg.getMessageData().getBytes(UTF_8);

        buffer.writeInt(HEADER_SIZE + data.length);
        buffer.writeInt(MAGIC_BYTES);

        SimulatorAddressCodec.encodeByteBuf(msg.getDestination(), buffer);
        SimulatorAddressCodec.encodeByteBuf(msg.getSource(), buffer);

        buffer.writeLong(msg.getMessageId());
        buffer.writeInt(msg.getMessageType());

        buffer.writeBytes(data);
    }

    public static SimulatorMessage decodeSimulatorMessage(ByteBuf buffer) {
        int frameLength = buffer.readInt();
        int dataLength = frameLength - HEADER_SIZE;

        if (buffer.readInt() != MAGIC_BYTES) {
            throw new IllegalArgumentException("Invalid magic bytes for SimulatorMessage");
        }

        SimulatorAddress destination = SimulatorAddressCodec.decodeSimulatorAddress(buffer);
        SimulatorAddress source = SimulatorAddressCodec.decodeSimulatorAddress(buffer);

        long messageId = buffer.readLong();
        int messageType = buffer.readInt();

        String messageData = buffer.readSlice(dataLength).toString(UTF_8);

        return new SimulatorMessage(destination, source, messageId, messageType, messageData);
    }

    public static int getDestinationAddressLevel(ByteBuf in) {
        return in.getInt(OFFSET_DST_ADDRESS);
    }

    public static int getChildAddressIndex(ByteBuf in, int addressLevelValue) {
        return in.getInt(OFFSET_DST_ADDRESS + ((addressLevelValue + 1) * INT_SIZE));
    }

    public static long getMessageId(ByteBuf in) {
        return in.getLong(OFFSET_MESSAGE_ID);
    }
}
