package com.hazelcast.simulator.protocol.operation;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.operation.OperationType.fromInt;
import static org.junit.Assert.assertEquals;

public class OperationTypeTest {

    @Test
    public void testFromInt_OPERATION() {
        assertEquals(OperationType.INTEGRATION_TEST, fromInt(OperationType.INTEGRATION_TEST.toInt()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromInt_invalid() {
        fromInt(-1);
    }
}
