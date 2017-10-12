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

/**
 * Encodes and decodes a {@link SimulatorAddress}.
 */
final class SimulatorAddressCodec {

    private SimulatorAddressCodec() {
    }

    static void encodeByteBuf(SimulatorAddress address, ByteBuf buffer) {
        buffer.writeInt(address.getAddressLevel().toInt())
                .writeInt(address.getAgentIndex())
                .writeInt(address.getWorkerIndex())
                .writeInt(address.getTestIndex());
    }

    static SimulatorAddress decodeSimulatorAddress(ByteBuf buffer) {
        return new SimulatorAddress(
                AddressLevel.fromInt(buffer.readInt()),
                buffer.readInt(),
                buffer.readInt(),
                buffer.readInt()
        );
    }
}
