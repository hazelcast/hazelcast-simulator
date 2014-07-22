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
    public void testGetMessageHelp() {
        String messageSpecs = Message.getMessageHelp();
        assertThat(messageSpecs.contains("dummyMessage"), is(true));
        assertThat(messageSpecs.contains("Dummy Runnable Message"), is(true));
    }

}