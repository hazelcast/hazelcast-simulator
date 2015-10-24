package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.common.KeyValuePair;
import org.junit.After;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class MessagesFactoryTest {

    @After
    public void tearDown() {
        deleteLogs();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBySpec_unknownMessageType() throws Exception {
        MessagesFactory.bySpec("foo", "Agent=*");
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
