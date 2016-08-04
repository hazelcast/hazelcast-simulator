package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class ClassUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ClassUtils.class);
    }
}
