package com.hazelcast.simulator.protocol.operation;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class SimulatorOperationFactoryTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(SimulatorOperationFactory.class);
    }
}
