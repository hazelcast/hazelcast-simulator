package com.hazelcast.stabilizer.common.messaging;

import org.junit.Test;

import static org.junit.Assert.*;

public class MessageTest {

    @Test(expected = IllegalArgumentException.class)
    public void message_address_cannot_be_null() {
       new Message(null);
    }

}