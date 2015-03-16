package com.hazelcast.simulator;

import com.hazelcast.simulator.utils.NativeUtils;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class NativeUtilsTest {

    @Test
    public void testGetPIDorNull() throws Exception {
        Integer pid = NativeUtils.getPIDorNull();
        assertNotNull(pid);
    }
}