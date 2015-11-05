package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.jclouds.ContextBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.createPublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deletePublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.getPrivateKeyFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.getPublicKeyFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.provisioner.ComputeServiceBuilder.ensurePublicPrivateKeyExist;
import static com.hazelcast.simulator.provisioner.ComputeServiceBuilder.newContextBuilder;
import static org.junit.Assert.assertNotNull;

public class ComputeServiceBuilderTest {

    @BeforeClass
    public static void setUp() {
        setDistributionUserDir();
        createPublicPrivateKeyFiles();
    }

    @AfterClass
    public static void tearDown() {
        resetUserDir();
        deletePublicPrivateKeyFiles();
    }

    @Test
    public void testConstructor() {
        SimulatorProperties simulatorProperties = new SimulatorProperties();
        new ComputeServiceBuilder(simulatorProperties);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_withNullProperties() {
        new ComputeServiceBuilder(null);
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

    @Test
    public void testNewContextBuilder() {
        ContextBuilder contextBuilder = newContextBuilder("aws-ec2");
        assertNotNull(contextBuilder);
    }

    @Test(expected = CommandLineExitException.class)
    public void testNewContextBuilder_invalidCloudProvider() {
        newContextBuilder("invalidCloudProvider");
    }
}
