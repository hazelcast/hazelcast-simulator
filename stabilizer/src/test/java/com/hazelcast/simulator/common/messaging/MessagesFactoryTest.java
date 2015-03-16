package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.common.KeyValuePair;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

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
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertNull(address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

    @Test
    public void testBySpec_withAttribute() throws Exception {
        KeyValuePair<String, String> attribute = new KeyValuePair<String, String>("foo", "bar");
        Message message = MessagesFactory.bySpec("dummyMessage", "Agent=*", attribute);
        MessageAddress address = message.getMessageAddress();

        assertEquals(DummyRunnableMessage.class, message.getClass());
        assertThat(((DummyRunnableMessage)message).getAttribute(), is(equalTo(attribute)));
        assertEquals(MessageAddress.BROADCAST, address.getAgentAddress());
        assertNull(address.getWorkerAddress());
        assertNull(address.getTestAddress());
    }

}