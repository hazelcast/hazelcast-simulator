package com.hazelcast.simulator.common.messaging;

import org.junit.After;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class MessageTest {

    @After
    public void tearDown() {
        deleteQuiet(new File("./logs"));
    }

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
