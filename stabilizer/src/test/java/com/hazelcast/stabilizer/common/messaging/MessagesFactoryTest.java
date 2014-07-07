package com.hazelcast.stabilizer.common.messaging;

import org.junit.Test;

import static org.junit.Assert.*;

public class MessagesFactoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void testBySpec_unknownMessageType() throws Exception {
        MessagesFactory.bySpec("fooo", "Agent=*");
    }

    @Test
    public void testBySpec() throws Exception {
        Message message = MessagesFactory.bySpec("dummyMessage", "Agent=*");
        MessageAddress address = message.getMessageAddress();

        assertEquals(DummyRunnableMessage.class, message.getClass());
        assertEquals(MessageAddress.BROADCAST_PREFIX, address.getAgentAddress());
        assertNull(address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

}