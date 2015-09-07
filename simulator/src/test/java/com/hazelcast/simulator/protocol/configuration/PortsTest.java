package com.hazelcast.simulator.protocol.configuration;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class PortsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(Ports.class);
    }
}
