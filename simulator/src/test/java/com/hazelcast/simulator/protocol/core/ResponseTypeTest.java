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

    @Test(expected = IllegalArgumentException.class)
    public void testFromInt_invalid() {
        fromInt(-1);
    }
}
