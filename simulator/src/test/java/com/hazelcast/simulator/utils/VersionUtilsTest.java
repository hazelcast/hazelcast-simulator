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
package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VersionUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(VersionUtils.class);
    }

    @Test
    public void testMinVersion() {
        assertTrue(isMinVersion("3.4", "3.5"));
        assertTrue(isMinVersion("3.4", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.4", "3.5-EA"));
        assertTrue(isMinVersion("3.4", "3.5-RC"));
        assertTrue(isMinVersion("3.4", "3.5-RC1"));
        assertTrue(isMinVersion("3.4", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4", "3.5.1"));
        assertTrue(isMinVersion("3.4", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4", "3.5.1-EA"));
        assertTrue(isMinVersion("3.4", "3.5.1-RC"));
        assertTrue(isMinVersion("3.4", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.4", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(isMinVersion("3.4.2", "3.5"));
        assertTrue(isMinVersion("3.4.2", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.4.2", "3.5-EA"));
        assertTrue(isMinVersion("3.4.2", "3.5-RC"));
        assertTrue(isMinVersion("3.4.2", "3.5-RC1"));
        assertTrue(isMinVersion("3.4.2", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4.2", "3.5.1"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-EA"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-RC"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.4.2", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(isMinVersion("3.5", "3.5"));
        assertTrue(isMinVersion("3.5", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.5", "3.5-EA"));
        assertTrue(isMinVersion("3.5", "3.5-RC"));
        assertTrue(isMinVersion("3.5", "3.5-RC1"));
        assertTrue(isMinVersion("3.5", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5", "3.5.1"));
        assertTrue(isMinVersion("3.5", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5", "3.5.1-EA"));
        assertTrue(isMinVersion("3.5", "3.5.1-RC"));
        assertTrue(isMinVersion("3.5", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.5", "3.5.1-RC1-SNAPSHOT"));

        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-SNAPSHOT"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-EA"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-RC"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-RC1"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5-RC1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-SNAPSHOT"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-EA"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-RC"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-RC1"));
        assertTrue(isMinVersion("3.5-SNAPSHOT", "3.5.1-RC1-SNAPSHOT"));

        assertFalse(isMinVersion("3.5", "3.4"));
        assertFalse(isMinVersion("3.5", "3.4-SNAPSHOT"));
        assertFalse(isMinVersion("3.5", "3.4-EA"));
        assertFalse(isMinVersion("3.5", "3.4-RC"));
        assertFalse(isMinVersion("3.5", "3.4-RC1"));
        assertFalse(isMinVersion("3.5", "3.4-RC1-SNAPSHOT"));
        assertFalse(isMinVersion("3.5", "3.4.2"));
        assertFalse(isMinVersion("3.5", "3.4.2-SNAPSHOT"));
        assertFalse(isMinVersion("3.5", "3.4.2-EA"));
        assertFalse(isMinVersion("3.5", "3.4.2-RC"));
        assertFalse(isMinVersion("3.5", "3.4.2-RC1"));
        assertFalse(isMinVersion("3.5", "3.4.2-RC1-SNAPSHOT"));

        assertFalse(isMinVersion("3.6", "3.5"));
        assertFalse(isMinVersion("3.6", "3.5-SNAPSHOT"));
        assertFalse(isMinVersion("3.6", "3.5-EA"));
        assertFalse(isMinVersion("3.6", "3.5-RC"));
        assertFalse(isMinVersion("3.6", "3.5-RC1"));
        assertFalse(isMinVersion("3.6", "3.5-RC1-SNAPSHOT"));
        assertFalse(isMinVersion("3.6", "3.5.1"));
        assertFalse(isMinVersion("3.6", "3.5.1-SNAPSHOT"));
        assertFalse(isMinVersion("3.6", "3.5.1-EA"));
        assertFalse(isMinVersion("3.6", "3.5.1-RC"));
        assertFalse(isMinVersion("3.6", "3.5.1-RC1"));
        assertFalse(isMinVersion("3.6", "3.5.1-RC1-SNAPSHOT"));
    }
}
