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

import java.io.File;

import static com.hazelcast.simulator.utils.BuildInfoUtils.DEFAULT_FALLBACK_MAJOR_VERSION;
import static com.hazelcast.simulator.utils.BuildInfoUtils.DEFAULT_FALLBACK_MINOR_VERSION;
import static com.hazelcast.simulator.utils.BuildInfoUtils.DEFAULT_MAJOR_VERSION;
import static com.hazelcast.simulator.utils.BuildInfoUtils.DEFAULT_MINOR_VERSION;
import static com.hazelcast.simulator.utils.BuildInfoUtils.getHazelcastVersionFromJAR;
import static com.hazelcast.simulator.utils.BuildInfoUtils.getHazelcastVersionFromJarOrNull;
import static com.hazelcast.simulator.utils.BuildInfoUtils.getMajorVersion;
import static com.hazelcast.simulator.utils.BuildInfoUtils.getMinorVersion;
import static com.hazelcast.simulator.utils.BuildInfoUtils.isMinVersion;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BuildInfoUtilsTest {

    private static final String RESOURCE_PATH = "simulator/src/test/resources/";

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(BuildInfoUtils.class);
    }

    @Test
    public void testIsMinVersion() {
        assertTrue(isMinVersion("3.6"));
    }

    @Test
    public void testIsMinVersion_whenVersionCannotBeFound_thenReturnFalse() {
        assertFalse(isMinVersion("3.6", null));
    }

    @Test
    public void testGetHazelcastVersionFromJAR() {
        assertEquals("2342", getHazelcastVersionFromJAR(RESOURCE_PATH + "build-info-with-version-manifest.jar"));
    }

    @Test
    public void testGetHazelcastVersionFromJAR_whenNoJarFound_thenReturnDefault() {
        assertEquals(format("%d%d", DEFAULT_MAJOR_VERSION, DEFAULT_MINOR_VERSION), getHazelcastVersionFromJAR(RESOURCE_PATH));
    }

    @Test
    public void testGetMajorVersion() {
        assertEquals(DEFAULT_MAJOR_VERSION, getMajorVersion());
    }

    @Test
    public void testGetMajorVersion_whenVersionCannotBeFound_thenReturnDefaultVersion() {
        assertEquals(DEFAULT_FALLBACK_MAJOR_VERSION, getMajorVersion(null));
    }

    @Test
    public void testGetMinorVersion() {
        assertEquals(DEFAULT_MINOR_VERSION, getMinorVersion());
    }

    @Test
    public void testGetMinorVersion_whenVersionCannotBeFound_thenReturnDefaultVersion() {
        assertEquals(DEFAULT_FALLBACK_MINOR_VERSION, getMinorVersion(null));
    }

    @Test
    public void testGetHazelcastVersionFromJarOrNull_whenJarContainsHazelcastProperties() {
        File file = new File(RESOURCE_PATH + "build-info-with-hazelcast-runtime.jar").getAbsoluteFile();
        assertEquals("42.23", getHazelcastVersionFromJarOrNull(file.getAbsolutePath()));
    }

    @Test
    public void testGetHazelcastVersionFromJarOrNull() {
        File file = new File(RESOURCE_PATH + "build-info-with-version-manifest.jar").getAbsoluteFile();
        assertEquals("23.42-SNAPSHOT", getHazelcastVersionFromJarOrNull(file.getAbsolutePath()));
    }

    @Test
    public void testGetHazelcastVersionFromJarOrNull_whenJarContainsNoVersion_thenReturnNull() {
        File file = new File(RESOURCE_PATH + "build-info-no-version.jar").getAbsoluteFile();
        assertNull(getHazelcastVersionFromJarOrNull(file.getAbsolutePath()));
    }

    @Test
    public void testGetHazelcastVersionFromJarOrNull_whenNoJarFound_thenReturnNull() {
        File file = new File("simulator/src/main/java/*").getAbsoluteFile();
        assertNull(getHazelcastVersionFromJarOrNull(file.getAbsolutePath()));
    }

    @Test
    public void testGetHazelcastVersionFromJarOrNull_whenInvalidPath_thenReturnNull() {
        File file = new File("notExists/*").getAbsoluteFile();
        assertNull(getHazelcastVersionFromJarOrNull(file.getAbsolutePath()));
    }
}
