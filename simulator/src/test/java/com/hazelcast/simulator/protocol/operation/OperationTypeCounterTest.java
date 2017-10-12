/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class OperationTypeCounterTest {

    @Before
    public void before() {
        OperationTypeCounter.reset();
    }

    @After
    public void after() {
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
