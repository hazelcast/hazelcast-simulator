package com.hazelcast.simulator.common;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.OUT_OF_THE_BOX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimulatorPropertiesTest {

    private final SimulatorProperties simulatorProperties = new SimulatorProperties();

    private File workingDirFile;
    private File customFile;

    @BeforeClass
    public static void setUp() {
        setDistributionUserDir();
    }

    @AfterClass
    public static void tearDown() {
        resetUserDir();
    }

    @Before
    public void initFiles() {
        workingDirFile = new File(SimulatorProperties.PROPERTIES_FILE_NAME);
        customFile = new File("custom.properties");
    }

    @After
    public void cleanup() {
        deleteQuiet(customFile);
        deleteQuiet(workingDirFile);
    }

    @Test
    public void testInit_defaults() {
        simulatorProperties.init(null);
    }

    @Test
    public void testInit_workingDir() {
        appendText("USER=workingDirUserName", workingDirFile);

        simulatorProperties.init(null);

        assertEquals("workingDirUserName", simulatorProperties.getUser());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_customFile_notExists() {
        simulatorProperties.init(customFile);
    }

    @Test
    public void testInit_customFile() {
        appendText("USER=testUserName", customFile);

        simulatorProperties.init(customFile);

        assertEquals("testUserName", simulatorProperties.getUser());
    }

    @Test(expected = RuntimeException.class)
    public void testLoad_notFound() {
        simulatorProperties.load(workingDirFile);
    }

    @Test
    public void testLoad_emptyValue() {
        appendText("FOO=", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertTrue(simulatorProperties.get("FOO").isEmpty());
    }

    @Test
    public void testLoad_justKey() {
        appendText("FOO", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertTrue(simulatorProperties.get("FOO").isEmpty());
    }

    @Test
    public void testLoad_specialCharKey() {
        appendText("%!)($!=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertEquals("value", simulatorProperties.get("%!)($!"));
    }

    @Test
    public void testLoad_UTFf8Key() {
        appendText("周楳=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertNull(simulatorProperties.get("周楳"));
    }

    @Test
    public void testLoad_comment() {
        appendText("#ignoredMe=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertNull(simulatorProperties.get("#ignoredMe"));
    }

    @Test
    public void testForceGit_null() {
        simulatorProperties.forceGit(null);
    }

    @Test
    public void testForceGit() {
        simulatorProperties.forceGit("hazelcast/master");

        assertEquals("git=hazelcast/master", simulatorProperties.getHazelcastVersionSpec());
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
    public void testGetHazelcastVersionSpec() {
        assertEquals(OUT_OF_THE_BOX, simulatorProperties.getHazelcastVersionSpec());
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
    public void testGet_systemProperty() {
        assertEquals(File.pathSeparator, simulatorProperties.get("path.separator", "ignored"));
    }

    @Test
    public void testGet_default_isIgnored() {
        assertEquals("simulator", simulatorProperties.get("USER", "ignored"));
    }

    @Test
    public void testGet_default_isUsed() {
        assertEquals("defaultValue", simulatorProperties.get("notFound", "defaultValue"));
    }

    @Test
    public void testGet_CLOUD_IDENTITY() {
        appendText("testCloudIdentityString", customFile);
        initProperty("CLOUD_IDENTITY", customFile.getName());

        assertEquals("testCloudIdentityString", simulatorProperties.getCloudIdentity());
    }

    @Test(expected = CommandLineExitException.class)
    public void testGet_CLOUD_IDENTITY_notFound() {
        initProperty("CLOUD_IDENTITY", "notFound");

        simulatorProperties.getCloudIdentity();
    }

    @Test
    public void testGet_CLOUD_CREDENTIAL() {
        appendText("testCloudCredentialString", customFile);
        initProperty("CLOUD_CREDENTIAL", customFile.getName());

        assertEquals("testCloudCredentialString", simulatorProperties.getCloudCredential());
    }

    @Test(expected = CommandLineExitException.class)
    public void testGet_CLOUD_CREDENTIAL_notFound() {
        initProperty("CLOUD_CREDENTIAL", "notFound");

        simulatorProperties.getCloudCredential();
    }

    private void initProperty(String key, Object value) {
        appendText(key + '=' + value, workingDirFile);
        simulatorProperties.init(null);
    }
}
