package com.hazelcast.stabilizer;

import org.junit.Test;

import static org.junit.Assert.*;

public class NativeUtilsTest {

    @Test
    public void testGetPIDorNull() throws Exception {
        Integer pid = NativeUtils.getPIDorNull();
        assertNotNull(pid);
    }

}