package com.hazelcast.simulator.protocol.registry;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.registry.TargetType.ALL;
import static com.hazelcast.simulator.protocol.registry.TargetType.CLIENT;
import static com.hazelcast.simulator.protocol.registry.TargetType.MEMBER;
import static com.hazelcast.simulator.protocol.registry.TargetType.PREFER_CLIENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TargetTypeTest {

    @Test
    public void testResolvePreferClient() {
        assertEquals(ALL, ALL.resolvePreferClient(true));
        assertEquals(ALL, ALL.resolvePreferClient(false));

        assertEquals(MEMBER, MEMBER.resolvePreferClient(true));
        assertEquals(MEMBER, MEMBER.resolvePreferClient(false));

        assertEquals(CLIENT, CLIENT.resolvePreferClient(true));
        assertEquals(CLIENT, CLIENT.resolvePreferClient(false));

        assertEquals(CLIENT, PREFER_CLIENT.resolvePreferClient(true));
        assertEquals(MEMBER, PREFER_CLIENT.resolvePreferClient(false));
    }

    @Test
    public void testMatchWorkerType() {
        assertTrue(ALL.matches(true));
        assertTrue(ALL.matches(false));

        assertTrue(MEMBER.matches(true));
        assertFalse(MEMBER.matches(false));

        assertFalse(CLIENT.matches(true));
        assertTrue(CLIENT.matches(false));
    }

    @Test
    public void testToString() {
        assertEquals("all", ALL.toString(0));
        assertEquals("3", ALL.toString(3));

        assertEquals("all member", MEMBER.toString(0));
        assertEquals("2 member", MEMBER.toString(2));

        assertEquals("all client", CLIENT.toString(0));
        assertEquals("4 client", CLIENT.toString(4));
    }
}
