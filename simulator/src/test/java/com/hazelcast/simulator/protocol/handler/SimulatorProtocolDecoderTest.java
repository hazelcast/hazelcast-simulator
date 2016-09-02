package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;

public class SimulatorProtocolDecoderTest {

    private SimulatorProtocolDecoder simulatorProtocolDecoder;

    private ByteBuf buffer;

    @Before
    public void before() {
        simulatorProtocolDecoder = new SimulatorProtocolDecoder(SimulatorAddress.COORDINATOR);
    }

    @After
    public void after() {
        if (buffer != null) {
            buffer.release();
        }
    }

    @Test
    public void testDecode_withEmptyBuffer() throws Exception {
        simulatorProtocolDecoder.decode(null, EMPTY_BUFFER, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecode_invalidBytes() throws Exception {
        buffer = Unpooled.buffer().capacity(8);
        buffer.writeInt(2);
        buffer.writeInt(0);
        buffer.resetReaderIndex();

        simulatorProtocolDecoder.decode(null, buffer, null);
    }
}
