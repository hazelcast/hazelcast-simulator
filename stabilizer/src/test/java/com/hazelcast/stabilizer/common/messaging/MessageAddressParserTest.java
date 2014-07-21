package com.hazelcast.stabilizer.common.messaging;

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
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getAgentAddress());
        assertNull(address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test
    public void testParse_toSingleRandomAgent() throws Exception {
        MessageAddress address = parser.parse("Agent=R");
        assertEquals(MessageAddress.RANDOM_PREFIX, address.getAgentAddress());
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
        assertEquals(MessageAddress.RANDOM_PREFIX, address.getAgentAddress());
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test
    public void testParse_toRandomWorker() throws Exception {
        MessageAddress address = parser.parse("Agent=*,Worker=R");
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getAgentAddress());
        assertEquals(MessageAddress.RANDOM_PREFIX, address.getWorkerAddress());
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
        assertEquals(MessageAddress.RANDOM_PREFIX, address.getAgentAddress());
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getWorkerAddress());
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getTestAddress());
    }

    @Test
    public void testParse_toRandomTest() throws Exception {
        MessageAddress address = parser.parse("Agent=*,Worker=R,Test=R");
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getAgentAddress());
        assertEquals(MessageAddress.RANDOM_PREFIX, address.getWorkerAddress());
        assertEquals(MessageAddress.RANDOM_PREFIX, address.getTestAddress());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParse_addressTooLong() throws Exception {
        parser.parse("Agent=*,Worker=R,Test=R,");
    }
}