package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.operation.OperationType.OperationTypeRegistry;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.operation.OperationType.INTEGRATION_TEST;
import static com.hazelcast.simulator.protocol.operation.OperationType.fromInt;
import static org.junit.Assert.assertEquals;

public class OperationTypeTest {

    @Test
    public void testFromInt_OPERATION() {
        assertEquals(INTEGRATION_TEST, fromInt(INTEGRATION_TEST.toInt()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromInt_invalid() {
        fromInt(-1);
    }

    @Test
    public void testGetClassType() {
        assertEquals(IntegrationTestOperation.class, INTEGRATION_TEST.getClassType());
    }

    @Test
    public void testGetOperationType() {
        SimulatorOperation operation = new IntegrationTestOperation();
        OperationType operationType = OperationType.getOperationType(operation);

        assertEquals(OperationType.INTEGRATION_TEST, operationType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOperationType_unregisteredOperationType() {
        SimulatorOperation operation = new UnregisteredOperation();
        OperationType.getOperationType(operation);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRegistry_classIdNegative() {
        OperationTypeRegistry.register(INTEGRATION_TEST, UnregisteredOperation.class, -1);
    }

    @Test(expected = IllegalStateException.class)
    public void testRegistry_classTypeAlreadyRegistered() {
        OperationTypeRegistry.register(INTEGRATION_TEST, IntegrationTestOperation.class, Integer.MAX_VALUE);
    }

    @Test(expected = IllegalStateException.class)
    public void testRegistry_ClassIdAlreadyRegistered() {
        OperationTypeRegistry.register(INTEGRATION_TEST, UnregisteredOperation.class, INTEGRATION_TEST.toInt());
    }

    private static class UnregisteredOperation implements SimulatorOperation {
    }
}
