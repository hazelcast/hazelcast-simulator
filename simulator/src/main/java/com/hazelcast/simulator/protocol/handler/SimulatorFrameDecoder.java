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
package com.hazelcast.simulator.protocol.handler;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static com.hazelcast.simulator.protocol.core.BaseCodec.INT_SIZE;

/**
 * Splits the received {@link io.netty.buffer.ByteBuf}s dynamically by the value of the length field in the message.
 */
public class SimulatorFrameDecoder extends LengthFieldBasedFrameDecoder {

    private static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
    private static final int LENGTH_FIELD_OFFSET = 0;
    private static final int LENGTH_FIELD_SIZE = INT_SIZE;

    public SimulatorFrameDecoder() {
        super(MAX_FRAME_SIZE, LENGTH_FIELD_OFFSET, LENGTH_FIELD_SIZE);
    }
}
