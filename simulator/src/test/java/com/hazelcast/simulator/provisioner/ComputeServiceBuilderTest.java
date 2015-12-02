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
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.provisioner.ComputeServiceBuilder.ensurePublicPrivateKeyExist;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ComputeServiceBuilderTest {

    @BeforeClass
    public static void setUp() {
        setLogLevel(Level.DEBUG);
        setDistributionUserDir();
        createCloudCredentialFiles();
        createPublicPrivateKeyFiles();
    }

    @AfterClass
    public static void tearDown() {
        resetLogLevel();
        resetUserDir();
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
        simulatorProperties.set("CLOUD_PROVIDER", CloudProviderUtils.PROVIDER_EC2);

        ComputeServiceBuilder builder = new ComputeServiceBuilder(simulatorProperties);
        assertNotNull(builder.build());
    }

    @Test
    public void testBuild_withStaticProvider() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.set("CLOUD_PROVIDER", CloudProviderUtils.PROVIDER_STATIC);

        ComputeServiceBuilder builder = new ComputeServiceBuilder(simulatorProperties);
        assertNull(builder.build());
    }

    @Test(expected = CommandLineExitException.class)
    public void testBuild_invalidCloudProvider() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        simulatorProperties.set("CLOUD_PROVIDER", "invalidCloudProvider");

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
