package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.fromInt;
import static org.junit.Assert.assertEquals;

public class ResponseTypeTest {

    @Test
    public void testFromInt_SUCCESS() {
        assertEquals(SUCCESS, fromInt(SUCCESS.toInt()));
    }

    @Test
    public void testFromInt_FAILURE_AGENT_NOT_FOUND() {
        assertEquals(FAILURE_AGENT_NOT_FOUND, fromInt(FAILURE_AGENT_NOT_FOUND.toInt()));
    }

    @Test
    public void testFromInt_FAILURE_WORKER_NOT_FOUND() {
        assertEquals(FAILURE_WORKER_NOT_FOUND, fromInt(FAILURE_WORKER_NOT_FOUND.toInt()));
    }

    @Test
    public void testFromInt_FAILURE_TEST_NOT_FOUND() {
        assertEquals(FAILURE_TEST_NOT_FOUND, fromInt(FAILURE_TEST_NOT_FOUND.toInt()));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testFromInt_invalid() {
        fromInt(-1);
    }
}
