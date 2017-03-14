package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.AddressLevel.fromInt;
import static org.junit.Assert.assertEquals;

public class AddressLevelTest {

    @Test
    public void testFromInt_COORDINATOR() {
        assertEquals(AddressLevel.COORDINATOR, fromInt(AddressLevel.COORDINATOR.toInt()));
    }

    @Test
    public void testFromInt_AGENT() {
        assertEquals(AddressLevel.AGENT, fromInt(AddressLevel.AGENT.toInt()));
    }

    @Test
    public void testFromInt_WORKER() {
        assertEquals(AddressLevel.WORKER, fromInt(AddressLevel.WORKER.toInt()));
    }

    @Test
    public void testFromInt_TEST() {
        assertEquals(AddressLevel.TEST, fromInt(AddressLevel.TEST.toInt()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromInt_tooLow() {
        fromInt(AddressLevel.getMinLevel() - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromInt_tooHigh() {
        fromInt(AddressLevel.getMaxLevel() + 1);
    }
}
