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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.createCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.createPublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deletePublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.getPrivateKeyFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.getPublicKeyFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.provisioner.ComputeServiceBuilder.ensurePublicPrivateKeyExist;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ComputeServiceBuilderTest {

    @BeforeClass
    public static void beforeClass() {
        setLogLevel(Level.DEBUG);
        setupFakeEnvironment();
        createCloudCredentialFiles();
        createPublicPrivateKeyFiles();
    }

    @AfterClass
    public static void afterClass() {
        resetLogLevel();
        tearDownFakeEnvironment();
        deleteCloudCredentialFiles();
        deletePublicPrivateKeyFiles();
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullProperties() {
        new ComputeServiceBuilder(null);
    }

    @Test
    public void testBuild() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.setCloudProvider(CloudProviderUtils.PROVIDER_EC2);

        ComputeServiceBuilder builder = new ComputeServiceBuilder(simulatorProperties);
        assertNotNull(builder.build());
    }

    @Test
    public void testBuild_withStaticProvider() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.setCloudProvider(CloudProviderUtils.PROVIDER_STATIC);

        ComputeServiceBuilder builder = new ComputeServiceBuilder(simulatorProperties);
        assertNull(builder.build());
    }

    @Test(expected = CommandLineExitException.class)
    public void testBuild_invalidCloudProvider() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.setCloudProvider("invalidCloudProvider");

        ComputeServiceBuilder builder = new ComputeServiceBuilder(simulatorProperties);
        assertNull(builder.build());
    }

    @Test
    public void testEnsurePublicPrivateKeyExist() {
        ensurePublicPrivateKeyExist(getPublicKeyFile(), getPrivateKeyFile());
    }

    @Test(expected = CommandLineExitException.class)
    public void testEnsurePublicPrivateKeyExist_noPublicKeyFile() {
        ensurePublicPrivateKeyExist(new File("notFound"), getPrivateKeyFile());
    }

    @Test(expected = CommandLineExitException.class)
    public void testEnsurePublicPrivateKeyExist_noPrivateKeyFile() {
        ensurePublicPrivateKeyExist(getPublicKeyFile(), new File("notFound"));
    }
}
