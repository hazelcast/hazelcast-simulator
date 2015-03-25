package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.NativeUtils.getPIDorNull;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;

public class NativeUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(NativeUtils.class);
    }

    @Test
    public void testGetPIDorNull() throws Exception {
        Integer pid = getPIDorNull();
        assertNotNull(pid);
    }
}
