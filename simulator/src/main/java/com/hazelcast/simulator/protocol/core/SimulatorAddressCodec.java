package com.hazelcast.simulator.protocol.core;

import io.netty.buffer.ByteBuf;

/**
 * Encodes and decodes a {@link SimulatorAddress}.
 */
final class SimulatorAddressCodec {

    private SimulatorAddressCodec() {
    }

    public static void encodeByteBuf(SimulatorAddress address, ByteBuf buffer) {
        buffer.writeInt(address.getAddressLevel().toInt())
                .writeInt(address.getAgentIndex())
                .writeInt(address.getWorkerIndex())
                .writeInt(address.getTestIndex());
    }

    public static SimulatorAddress decodeSimulatorAddress(ByteBuf buffer) {
        return new SimulatorAddress(
                AddressLevel.fromInt(buffer.readInt()),
                buffer.readInt(),
                buffer.readInt(),
                buffer.readInt()
        );
    }
}
