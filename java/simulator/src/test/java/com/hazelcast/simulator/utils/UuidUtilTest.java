package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.UUID;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUUID;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUUID;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UuidUtilTest {

    private static final int UUID_LENGTH = 36;
    private static final int UUID_VERSION = 4;

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(UuidUtil.class);
    }

    @Test
    public void testNewUnsecureUuidString() {
        String uuid = newUnsecureUuidString();

        assertNotNull(uuid);
        assertEquals(UUID_LENGTH, uuid.length());
    }

    @Test
    public void testNewSecureUuidString() {
        String uuid = newSecureUuidString();

        assertNotNull(uuid);
        assertEquals(UUID_LENGTH, uuid.length());
    }

    @Test
    public void testNewUnsecureUUID() {
        UUID uuid = newUnsecureUUID();

        assertNotNull(uuid);
        assertEquals(UUID_VERSION, uuid.version());
        assertEquals(UUID_LENGTH, uuid.toString().length());
    }

    @Test
    public void testNewSecureUUID() {
        UUID uuid = newSecureUUID();

        assertNotNull(uuid);
        assertEquals(UUID_VERSION, uuid.version());
        assertEquals(UUID_LENGTH, uuid.toString().length());
    }
}
