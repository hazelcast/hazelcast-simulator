package com.hazelcast.simulator.protocol.operation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class OperationTypeCounterTest {

    @Before
    public void setUp() {
        OperationTypeCounter.reset();
    }

    @After
    public void tearDown() {
        OperationTypeCounter.reset();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(OperationTypeCounter.class);
    }

    @Test
    public void testSent() {
        OperationTypeCounter.sent(OperationType.CREATE_WORKER);
        OperationTypeCounter.sent(OperationType.CREATE_TEST);
        OperationTypeCounter.sent(OperationType.CREATE_TEST);

        assertEquals(1, OperationTypeCounter.getSent(OperationType.CREATE_WORKER));
        assertEquals(2, OperationTypeCounter.getSent(OperationType.CREATE_TEST));
    }

    @Test
    public void testReceived() {
        OperationTypeCounter.received(OperationType.START_TEST);
        OperationTypeCounter.received(OperationType.START_TEST_PHASE);
        OperationTypeCounter.received(OperationType.START_TEST_PHASE);

        assertEquals(1, OperationTypeCounter.getReceived(OperationType.START_TEST));
        assertEquals(2, OperationTypeCounter.getReceived(OperationType.START_TEST_PHASE));
    }

    @Test
    public void testPrintStatistics() {
        OperationTypeCounter.printStatistics();
    }
}
