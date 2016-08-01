package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class PreconditionsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(Preconditions.class);
    }
}
