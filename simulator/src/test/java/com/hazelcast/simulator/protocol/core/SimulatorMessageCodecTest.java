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
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.SimulatorMessageCodec.decodeSimulatorMessage;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class SimulatorMessageCodecTest {

    private ByteBuf buffer;

    @After
    public void after() {
        if (buffer != null) {
            buffer.release();
        }
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(SimulatorMessageCodec.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void decodeSimulatorMessage_invalidMagicBytes() {
        buffer = Unpooled.buffer().capacity(8);
        buffer.writeInt(8);
        buffer.writeInt(0);
        buffer.resetReaderIndex();

        decodeSimulatorMessage(buffer);
    }
}
