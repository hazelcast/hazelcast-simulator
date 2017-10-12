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
package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.UUID;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUUID;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUUID;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUuidString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UuidUtilTest {

    private static final int UUID_LENGTH = 36;
    private static final int UUID_VERSION = 4;

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(UuidUtil.class);
    }

    @Test
    public void testNewUnsecureUuidString() {
        String uuid = newUnsecureUuidString();

        assertNotNull(uuid);
        assertEquals(UUID_LENGTH, uuid.length());
    }

    @Test
    public void testNewSecureUuidString() {
        String uuid = newSecureUuidString();

        assertNotNull(uuid);
        assertEquals(UUID_LENGTH, uuid.length());
    }

    @Test
    public void testNewUnsecureUUID() {
        UUID uuid = newUnsecureUUID();

        assertNotNull(uuid);
        assertEquals(UUID_VERSION, uuid.version());
        assertEquals(UUID_LENGTH, uuid.toString().length());
    }

    @Test
    public void testNewSecureUUID() {
        UUID uuid = newSecureUUID();

        assertNotNull(uuid);
        assertEquals(UUID_VERSION, uuid.version());
        assertEquals(UUID_LENGTH, uuid.toString().length());
    }
}
