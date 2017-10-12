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
package com.hazelcast.simulator.protocol.registry;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.registry.TargetType.ALL;
import static com.hazelcast.simulator.protocol.registry.TargetType.CLIENT;
import static com.hazelcast.simulator.protocol.registry.TargetType.MEMBER;
import static com.hazelcast.simulator.protocol.registry.TargetType.PREFER_CLIENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TargetTypeTest {

    @Test
    public void testResolvePreferClient() {
        assertEquals(ALL, ALL.resolvePreferClient(true));
        assertEquals(ALL, ALL.resolvePreferClient(false));

        assertEquals(MEMBER, MEMBER.resolvePreferClient(true));
        assertEquals(MEMBER, MEMBER.resolvePreferClient(false));

        assertEquals(CLIENT, CLIENT.resolvePreferClient(true));
        assertEquals(CLIENT, CLIENT.resolvePreferClient(false));

        assertEquals(CLIENT, PREFER_CLIENT.resolvePreferClient(true));
        assertEquals(MEMBER, PREFER_CLIENT.resolvePreferClient(false));
    }

    @Test
    public void testMatchWorkerType() {
        assertTrue(ALL.matches(true));
        assertTrue(ALL.matches(false));

        assertTrue(MEMBER.matches(true));
        assertFalse(MEMBER.matches(false));

        assertFalse(CLIENT.matches(true));
        assertTrue(CLIENT.matches(false));
    }

    @Test
    public void testToString() {
        assertEquals("all Workers", ALL.toString(0));
        assertEquals("3 Workers", ALL.toString(3));

        assertEquals("all member Workers", MEMBER.toString(0));
        assertEquals("1 member Worker", MEMBER.toString(1));

        assertEquals("all client Workers", CLIENT.toString(0));
        assertEquals("4 client Workers", CLIENT.toString(4));
    }
}
