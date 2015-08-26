package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimulatorProtocolDecoderTest {

    private SimulatorProtocolDecoder simulatorProtocolDecoder;

    private ByteBuf buffer;

    @Before
    public void setUp() {
        simulatorProtocolDecoder = new SimulatorProtocolDecoder(SimulatorAddress.COORDINATOR);
    }

    @After
    public void tearDown() {
        if (buffer != null) {
            buffer.release();
        }
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
