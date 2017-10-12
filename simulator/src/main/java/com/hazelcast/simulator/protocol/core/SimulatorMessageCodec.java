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

import com.hazelcast.simulator.protocol.operation.OperationType;
import io.netty.buffer.ByteBuf;

import static com.hazelcast.simulator.protocol.core.BaseCodec.ADDRESS_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;
import static com.hazelcast.simulator.protocol.core.BaseCodec.LONG_SIZE;
import static com.hazelcast.simulator.protocol.core.SimulatorAddressCodec.decodeSimulatorAddress;
import static io.netty.util.CharsetUtil.UTF_8;

/**
 * Encodes and decodes a {@link SimulatorMessage}.
 */
public final class SimulatorMessageCodec {

    private static final int MAGIC_BYTES = 0xA5E1CA57;

    private static final int OFFSET_MAGIC_BYTES = INT_SIZE;
    private static final int OFFSET_DST_ADDRESS = 2 * INT_SIZE;
    private static final int OFFSET_SRC_ADDRESS = OFFSET_DST_ADDRESS + ADDRESS_SIZE;
    private static final int OFFSET_MESSAGE_ID = OFFSET_SRC_ADDRESS + ADDRESS_SIZE;

    private static final int HEADER_SIZE = 2 * INT_SIZE + LONG_SIZE + 2 * ADDRESS_SIZE;

    private SimulatorMessageCodec() {
    }

    public static void encodeByteBuf(SimulatorMessage msg, ByteBuf buffer) {
        byte[] data = msg.getOperationData().getBytes(UTF_8);

        buffer.writeInt(HEADER_SIZE + data.length);
        buffer.writeInt(MAGIC_BYTES);

        SimulatorAddressCodec.encodeByteBuf(msg.getDestination(), buffer);
        SimulatorAddressCodec.encodeByteBuf(msg.getSource(), buffer);

        buffer.writeLong(msg.getMessageId());
        buffer.writeInt(msg.getOperationType().toInt());

        buffer.writeBytes(data);
    }

    public static SimulatorMessage decodeSimulatorMessage(ByteBuf buffer) {
        int frameLength = buffer.readInt();
        int dataLength = frameLength - HEADER_SIZE;

        if (buffer.readInt() != MAGIC_BYTES) {
            throw new IllegalArgumentException("Invalid magic bytes for SimulatorMessage");
        }

        SimulatorAddress destination = decodeSimulatorAddress(buffer);
        SimulatorAddress source = decodeSimulatorAddress(buffer);

        long messageId = buffer.readLong();
        OperationType operationType = OperationType.fromInt(buffer.readInt());

        String operationData = buffer.readSlice(dataLength).toString(UTF_8);

        return new SimulatorMessage(destination, source, messageId, operationType, operationData);
    }

    public static boolean isSimulatorMessage(ByteBuf in) {
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

    public static SimulatorAddress getSourceAddress(ByteBuf in) {
        return decodeSimulatorAddress(in.slice(OFFSET_SRC_ADDRESS, ADDRESS_SIZE));
    }
}
