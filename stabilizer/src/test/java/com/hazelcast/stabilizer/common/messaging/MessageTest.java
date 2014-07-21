package com.hazelcast.stabilizer.common.messaging;

import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class MessageTest {

    @Test(expected = IllegalArgumentException.class)
    public void message_address_cannot_be_null() {
       new Message(null) {

       };
    }

    @Test
    public void testGetMessageSpecs() {
        Set<String> messageSpecs = Message.getMessageSpecs();
        assertThat(messageSpecs, hasItem("dummyMessage"));
    }

}