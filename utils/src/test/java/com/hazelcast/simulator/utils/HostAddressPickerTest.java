package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;

public class HostAddressPickerTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(HostAddressPicker.class);
    }

    @Test
    public void testPickHostAddress() throws UnknownHostException {
        String hostAddress = HostAddressPicker.pickHostAddress();
        assertNotNull(hostAddress);
        assertNotNull(InetAddress.getByName(hostAddress));
    }
}