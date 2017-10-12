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

import com.hazelcast.simulator.common.SimulatorProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTY_CLOUD_CREDENTIAL;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTY_CLOUD_IDENTITY;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.isHarakiriMonitorEnabled;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HarakiriMonitorUtilsTest {

    private final SimulatorProperties properties = new SimulatorProperties();

    @BeforeClass
    public static void beforeClass() {
        setupFakeEnvironment();
    }

    @AfterClass
    public static void afterClass() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(HarakiriMonitorUtils.class);
    }

    @Test
    public void testIsEnabled_onStatic() {
        properties.setCloudProvider(PROVIDER_STATIC);

        assertFalse(isHarakiriMonitorEnabled(properties));
    }

    @Test
    public void testIsEnabled_onEC2() {
        properties.setCloudProvider(PROVIDER_EC2);

        assertTrue(isHarakiriMonitorEnabled(properties));
    }

    @Test
    public void testIsEnabled_onEC2_featureDisabled() {
        properties.setCloudProvider(PROVIDER_EC2);
        properties.set("HARAKIRI_MONITOR_ENABLED", "false");

        assertFalse(isHarakiriMonitorEnabled(properties));
    }

    @Test
    public void testIsEnabled_onEC2_equalsIgnoreCase() {
        properties.setCloudProvider(PROVIDER_EC2);
        properties.set("HARAKIRI_MONITOR_ENABLED", "TruE");

        assertTrue(isHarakiriMonitorEnabled(properties));
    }

    @Test
    public void testGetStartCommand() {
        properties.setCloudProvider(PROVIDER_EC2);
        properties.set(PROPERTY_CLOUD_IDENTITY, "identity");
        properties.set(PROPERTY_CLOUD_CREDENTIAL, "credential");

        File identity = ensureExistingFile("identity");
        File credentials = ensureExistingFile("credential");
        try {
            appendText("someIdentity", identity);
            appendText("someCredential", credentials);

            String command = getStartHarakiriMonitorCommandOrNull(properties);
            assertNotNull(command);
            assertTrue(command.contains("someIdentity"));
            assertTrue(command.contains("someCredential"));
        } finally {
            deleteQuiet(identity);
            deleteQuiet(credentials);
        }
    }

    @Test
    public void testGetStartCommand_onStatic() {
        properties.setCloudProvider(PROVIDER_STATIC);
        properties.set("HARAKIRI_MONITOR_ENABLED", "true");

        String command = getStartHarakiriMonitorCommandOrNull(properties);
        assertNull(command);
    }

    @Test
    public void testGetStartCommand_featureDisabled() {
        properties.setCloudProvider(PROVIDER_EC2);
        properties.set("HARAKIRI_MONITOR_ENABLED", "false");

        String command = getStartHarakiriMonitorCommandOrNull(properties);
        assertNull(command);
    }
}
