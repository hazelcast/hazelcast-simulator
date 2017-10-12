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

import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.SimulatorUtils.getPropertiesFile;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulatorUtilsTest {

    private File agentsFile;
    private ComponentRegistry componentRegistry;

    @Before
    public void before() {
        setupFakeEnvironment();
        agentsFile = ensureExistingFile(getUserDir(), "SimulatorUtilsTest-agentsFile.txt");
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(SimulatorUtils.class);
    }

    @Test
    public void testLoadComponentRegister() {
        writeText("192.168.1.1,10.10.10.10", agentsFile);

        componentRegistry = loadComponentRegister(agentsFile);
        assertEquals(1, componentRegistry.agentCount());
    }

    @Test(expected = CommandLineExitException.class)
    public void testLoadComponentRegister_emptyFile_withSizeCheck() {
        componentRegistry = loadComponentRegister(agentsFile, true);
    }

    @Test
    public void testLoadComponentRegister_emptyFile_withoutSizeCheck() {
        componentRegistry = loadComponentRegister(agentsFile, false);
        assertEquals(0, componentRegistry.agentCount());
    }

    @Test
    public void testGetPropertiesFile() {
        OptionSet options = mock(OptionSet.class);
        when(options.has(any(OptionSpec.class))).thenReturn(true);
        when(options.valueOf(any(OptionSpec.class))).thenReturn("test");

        File expectedFile = new File("test");
        File actualFile = getPropertiesFile(options, null);

        assertEquals(expectedFile, actualFile);
    }

    @Test
    public void testGetPropertiesFile_noPropertiesSpec() {
        OptionSet options = mock(OptionSet.class);
        when(options.has(any(OptionSpec.class))).thenReturn(false);

        File actualFile = getPropertiesFile(options, null);

        assertEquals(null, actualFile);
    }
}
