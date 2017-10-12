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
package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.AddressLevel.fromInt;
import static org.junit.Assert.assertEquals;

public class AddressLevelTest {

    @Test
    public void testFromInt_REMOTE() {
        assertEquals(AddressLevel.REMOTE, fromInt(AddressLevel.REMOTE.toInt()));
    }

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
