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
    public void tearDown() {
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
