package com.hazelcast.simulator.common;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.CLOUD_CREDENTIAL;
import static com.hazelcast.simulator.common.SimulatorProperties.CLOUD_IDENTITY;
import static com.hazelcast.simulator.common.SimulatorProperties.CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimulatorPropertiesTest {

    private File simulatorHome;
    private SimulatorProperties simulatorProperties;

    @Before
    public void before() {
        this.simulatorHome = setupFakeEnvironment();
        this.simulatorProperties = new SimulatorProperties();
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testInit_defaults() {
        simulatorProperties.init((File) null);
    }

    @Test
    public void testInit_workingDir() {
        File workingDirProperties = new File(simulatorHome, "simulator.properties");

        appendText("SIMULATOR_USER=workingDirUserName", workingDirProperties);

        simulatorProperties.init(workingDirProperties);

        assertEquals("workingDirUserName", simulatorProperties.getUser());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_customFile_notExists() {
        File file = new File(simulatorHome, "simulator.properties");
        simulatorProperties.init(file);
    }

    @Test
    public void testInit_customFile() {
        File file = new File(simulatorHome, "simulator.properties");
        appendText("SIMULATOR_USER=testUserName", file);

        simulatorProperties.init(file);

        assertEquals("testUserName", simulatorProperties.getUser());
    }

    @Test(expected = RuntimeException.class)
    public void testLoad_notFound() {
        simulatorProperties.load(new File("notexisting"));
    }

    @Test
    public void testLoad_emptyValue() {
        File workingDirFile = new File(simulatorHome, "simulator.properties");

        appendText("FOO=", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertTrue(simulatorProperties.get("FOO").isEmpty());
    }

    @Test
    public void testLoad_trimming() {
        File workingDirFile = new File(simulatorHome, "simulator.properties");

        appendText("FOO= bar ", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertEquals("bar", simulatorProperties.get("FOO"));
    }

    @Test
    public void testLoad_justKey() {
        File workingDirFile = new File(simulatorHome, "simulator.properties");
        appendText("FOO", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertTrue(simulatorProperties.get("FOO").isEmpty());
    }

    @Test
    public void testLoad_specialCharKey() {
        File workingDirFile = new File(simulatorHome, "simulator.properties");
        appendText("%!)($!=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertEquals("value", simulatorProperties.get("%!)($!"));
    }

    @Test
    public void testLoad_UTFf8Key() {
        File workingDirFile = new File(simulatorHome, "simulator.properties");
        appendText("周楳=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertNull(simulatorProperties.get("周楳"));
    }

    @Test
    public void testLoad_comment() {
        File workingDirProperties = new File(simulatorHome, "simulator.properties");
        appendText("#ignoredMe=value", workingDirProperties);

        simulatorProperties.load(workingDirProperties);

        assertNull(simulatorProperties.get("#ignoredMe"));
    }

    @Test
    public void testContainsKey() {
        assertTrue(simulatorProperties.containsKey(CLOUD_PROVIDER));
        assertFalse(simulatorProperties.containsKey("UNKNOWN_PROPERTY"));
    }

    @Test
    public void testGetSshOptions() {
        assertNotNull(simulatorProperties.getSshOptions());
    }

    @Test
    public void testGetUser() {
        assertEquals("simulator", simulatorProperties.getUser());
    }

    @Test
    public void testGetVersionSpec() {
        assertEquals("outofthebox", simulatorProperties.getVersionSpec());
    }

    @Test
    public void testGet() {
        assertEquals("5701", simulatorProperties.get("HAZELCAST_PORT"));
    }

    @Test
    public void testGet_notFound() {
        assertNull(simulatorProperties.get("notFound"));
    }

    @Test
    public void testGet_GROUP_NAME() {
        assertNotNull(simulatorProperties.get("GROUP_NAME"));
    }

    @Test
    public void testGet_withSystemProperty() {
        assertEquals(File.pathSeparator, simulatorProperties.get("path.separator"));
    }

    @Test
    public void testGet_withSystemProperty_withDefaultValue() {
        assertEquals(File.pathSeparator, simulatorProperties.get("path.separator", "ignored"));
    }

    @Test
    public void testGet_withDefaultValue_defaultIsIgnored() {
        assertEquals("simulator", simulatorProperties.get("SIMULATOR_USER", "ignored"));
    }

    @Test
    public void testGet_withDefaultValue_defaultIsUsed() {
        assertEquals("defaultValue", simulatorProperties.get("notFound", "defaultValue"));
    }

    @Test
    public void testGetWorkerPingIntervalSeconds() {
        assertEquals(60, simulatorProperties.getWorkerPingIntervalSeconds());
    }

    @Test
    public void testGetMemberWorkerShutdownDelaySeconds() {
        assertEquals(5, simulatorProperties.getMemberWorkerShutdownDelaySeconds());
    }

    @Test
    public void testGetWorkerLastSeenTimeoutSeconds() {
        assertEquals(180, simulatorProperties.getWorkerLastSeenTimeoutSeconds());
    }

    @Test
    public void testGetCoordinatorPort() {
        assertEquals(5000, simulatorProperties.getCoordinatorPort());
    }

    @Test
    public void testGetAgentThreadPoolSize() {
        assertEquals(0, simulatorProperties.getAgentThreadPoolSize());
    }

    @Test
    public void testGet_CLOUD_IDENTITY() {
        File identityFile = new File(simulatorHome, "identity");
        appendText("testCloudIdentityString", identityFile);

        File customFile = new File(simulatorHome, "simulator.properties");
        initProperty(customFile, CLOUD_IDENTITY, identityFile.getAbsolutePath());

        assertEquals("testCloudIdentityString", simulatorProperties.getCloudIdentity());
    }

    @Test
    public void testGet_CLOUD_CREDENTIAL() {
        File credentialsFile = new File(simulatorHome, "credentials");
        appendText("testIdentity", credentialsFile);

        File customFile = new File(simulatorHome, "simulator.properties");
        initProperty(customFile, CLOUD_CREDENTIAL, credentialsFile.getAbsolutePath());

        assertEquals("testIdentity", simulatorProperties.getCloudCredential());
    }

    @Test(expected = CommandLineExitException.class)
    public void test_USER() {
        File workingDirProperties = new File(simulatorHome, "simulator.properties");
        appendText("USER=foobar", workingDirProperties);

        simulatorProperties.load(workingDirProperties);
    }

    private void initProperty(File file, String key, String value) {
        System.out.println("writing value: " + value);
        appendText(format("%s=%s", key, value), file);
        simulatorProperties.init(file);
    }
}
