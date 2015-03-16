package com.hazelcast.simulator.common.messaging;

import org.junit.Test;

import static org.junit.Assert.*;

public class MessageAddressParserTest {

    private MessageAddressParser parser = new MessageAddressParser();

    @Test(expected = IllegalArgumentException.class)
    public void testParse_nullAsInput() throws Exception {
        parser.parse(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_emptyInput() throws Exception {
        parser.parse("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_missingAgentPrefix() throws Exception {
        parser.parse("foooooooooooooooo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_missingAgentMode() throws Exception {
        parser.parse("Agent=");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_wrongAgentMode() throws Exception {
        parser.parse("Agent=A");
    }

    @Test
    public void testParse_toAllAgents() throws Exception {
        MessageAddress address = parser.parse("Agent=*");
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertNull(address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test
    public void testParse_toSingleRandomAgent() throws Exception {
        MessageAddress address = parser.parse("Agent=R");
        assertEquals(MessageAddress.RANDOM, address.getAgentAddress());
        assertNull(address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_missingWorker() throws Exception {
        parser.parse("Agent=R,");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_wrongWorkerFormat() throws Exception {
        parser.parse("Agent=R,Fooo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_wrongWorkerMode() throws Exception {
        parser.parse("Agent=R,Worker=A");
    }

    @Test
    public void testParse_toAllWorkers() throws Exception {
        MessageAddress address = parser.parse("Agent=R,Worker=*");
        assertEquals(MessageAddress.RANDOM, address.getAgentAddress());
        assertEquals(MessageAddress.BROADCAST, address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test
    public void testParse_toRandomWorker() throws Exception {
        MessageAddress address = parser.parse("Agent=*,Worker=R");
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertEquals(MessageAddress.RANDOM, address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_missingTest() throws Exception {
        parser.parse("Agent=R,Worker=*,");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_wrongTestFormat() throws Exception {
        parser.parse("Agent=R,Worker=*,Fooo");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_wrongTestMode() throws Exception {
        parser.parse("Agent=R,Worker=*,Test=A");
    }

    @Test
    public void testParse_toAllTest() throws Exception {
        MessageAddress address = parser.parse("Agent=R,Worker=*,Test=*");
        assertEquals(MessageAddress.RANDOM, address.getAgentAddress());
        assertEquals(MessageAddress.BROADCAST, address.getWorkerAddress());
        assertEquals(MessageAddress.BROADCAST, address.getTestAddress());
    }

    @Test
    public void testParse_toRandomTest() throws Exception {
        MessageAddress address = parser.parse("Agent=*,Worker=R,Test=R");
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertEquals(MessageAddress.RANDOM, address.getWorkerAddress());
        assertEquals(MessageAddress.RANDOM, address.getTestAddress());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_addressTooLong() throws Exception {
        parser.parse("Agent=*,Worker=R,Test=R,");
    }

    @Test
    public void testParse_toAllMembersWithWorker() {
        MessageAddress address = parser.parse("Agent=*,Worker=*m");
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertEquals(MessageAddress.ALL_WORKERS_WITH_MEMBER, address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test
    public void testParse_toRandomMemberWithWorker() {
        MessageAddress address = parser.parse("Agent=*,Worker=Rm");
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertEquals(MessageAddress.RANDOM_WORKER_WITH_MEMBER, address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }
}