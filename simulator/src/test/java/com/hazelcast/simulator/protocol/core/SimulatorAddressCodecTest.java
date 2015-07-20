package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class SimulatorAddressCodecTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(SimulatorAddressCodec.class);
    }
}
