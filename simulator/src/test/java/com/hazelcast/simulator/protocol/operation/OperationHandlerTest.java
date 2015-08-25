package com.hazelcast.simulator.protocol.operation;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class OperationHandlerTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(OperationHandler.class);
    }
}
