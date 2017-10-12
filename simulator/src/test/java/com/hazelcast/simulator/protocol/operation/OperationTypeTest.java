/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
