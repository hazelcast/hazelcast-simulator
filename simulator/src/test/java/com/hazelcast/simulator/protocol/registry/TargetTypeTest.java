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
    public void testIsMemberTarget() {
        assertTrue(ALL.isMemberTarget());
        assertTrue(MEMBER.isMemberTarget());
        assertFalse(CLIENT.isMemberTarget());
        assertFalse(PREFER_CLIENT.isMemberTarget());
    }

    @Test
    public void testResolvePreferClients() {
        assertEquals(ALL, ALL.resolvePreferClients(true));
        assertEquals(ALL, ALL.resolvePreferClients(false));

        assertEquals(MEMBER, MEMBER.resolvePreferClients(true));
        assertEquals(MEMBER, MEMBER.resolvePreferClients(false));

        assertEquals(CLIENT, CLIENT.resolvePreferClients(true));
        assertEquals(CLIENT, CLIENT.resolvePreferClients(false));

        assertEquals(CLIENT, PREFER_CLIENT.resolvePreferClients(true));
        assertEquals(MEMBER, PREFER_CLIENT.resolvePreferClients(false));
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
