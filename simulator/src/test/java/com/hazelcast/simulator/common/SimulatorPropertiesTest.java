package com.hazelcast.simulator.common;

import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimulatorPropertiesTest {

    private String userDir;
    private SecurityManager oldSecurityManager;
    private SimulatorProperties simulatorProperties;

    private File workingDirFile = new File(SimulatorProperties.PROPERTIES_FILE_NAME);
    private File customFile = new File("custom.properties");

    @Before
    public void setUp() throws Exception {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager());

        userDir = System.getProperty("user.dir");
        System.setProperty("user.dir", "./dist/src/main/dist");
        simulatorProperties = new SimulatorProperties();
        System.setProperty("user.dir", userDir);
    }

    @After
    public void tearDown() {
        System.setSecurityManager(oldSecurityManager);

        deleteQuiet(customFile);
        deleteQuiet(workingDirFile);
    }

    @Test
    public void testInit_defaults() throws Exception {
        simulatorProperties.init(null);
    }

    @Test
    public void testInit_workingDir() throws Exception {
        appendText("USER=workingDirUserName", workingDirFile);

        simulatorProperties.init(null);

        assertEquals("workingDirUserName", simulatorProperties.getUser());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_customFile_notExists() throws Exception {
        simulatorProperties.init(customFile);
    }

    @Test
    public void testInit_customFile() throws Exception {
        appendText("USER=testUserName", customFile);

        simulatorProperties.init(customFile);

        assertEquals("testUserName", simulatorProperties.getUser());
    }

    @Test(expected = RuntimeException.class)
    public void testLoad_notFound() throws Exception {
        simulatorProperties.load(workingDirFile);
    }

    @Test
    public void testLoad_emptyValue() throws Exception {
        appendText("USER=", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertTrue(simulatorProperties.get("USER").isEmpty());
    }

    @Test
    public void testLoad_justKey() throws Exception {
        appendText("USER", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertTrue(simulatorProperties.get("USER").isEmpty());
    }

    @Test
    public void testLoad_specialCharKey() throws Exception {
        appendText("%!)($!=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertEquals("value", simulatorProperties.get("%!)($!"));
    }

    @Test
    public void testLoad_UTFf8Key() throws Exception {
        appendText("周楳=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertNull(simulatorProperties.get("周楳"));
    }

    @Test
    public void testLoad_comment() throws Exception {
        appendText("#ignoredMe=value", workingDirFile);

        simulatorProperties.load(workingDirFile);

        assertNull(simulatorProperties.get("#ignoredMe"));
    }

    @Test
    public void testForceGit_null() throws Exception {
        simulatorProperties.forceGit(null);
    }

    @Test
    public void testForceGit() throws Exception {
        simulatorProperties.forceGit("hazelcast/master");

        assertEquals("git=hazelcast/master", simulatorProperties.getHazelcastVersionSpec());
    }

    @Test
    public void testIsEC2() throws Exception {
        assertTrue(simulatorProperties.isEC2());
    }

    @Test
    public void testGetUser() throws Exception {
        assertEquals("simulator", simulatorProperties.getUser());
    }

    @Test
    public void testGetHazelcastVersionSpec() throws Exception {
        assertTrue(simulatorProperties.getHazelcastVersionSpec().equals("outofthebox"));
    }

    @Test
    public void testGet() throws Exception {
        assertEquals("simulator", simulatorProperties.get("USER"));
    }

    @Test
    public void testGet_notFound() throws Exception {
        assertNull(simulatorProperties.get("notFound"));
    }

    @Test
    public void testGet_GROUP_NAME() throws Exception {
        assertNotNull(simulatorProperties.get("GROUP_NAME"));
    }

    @Test
    public void testGet_systemProperty() throws Exception {
        assertEquals(userDir, simulatorProperties.get("user.dir", "ignored"));
    }

    @Test
    public void testGet_default_isIgnored() throws Exception {
        assertEquals("simulator", simulatorProperties.get("USER", "ignored"));
    }

    @Test
    public void testGet_default_isUsed() throws Exception {
        assertEquals("defaultValue", simulatorProperties.get("notFound", "defaultValue"));
    }

    @Test
    public void testGet_CLOUD_IDENTITY() throws Exception {
        appendText("testCloudIdentityString", customFile);
        initProperty("CLOUD_IDENTITY", customFile.getAbsolutePath());

        assertEquals("testCloudIdentityString", simulatorProperties.get("CLOUD_IDENTITY"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testGet_CLOUD_IDENTITY_notFound() throws Exception {
        initProperty("CLOUD_IDENTITY", "notFound");

        simulatorProperties.get("CLOUD_IDENTITY");
    }

    @Test
    public void testGet_CLOUD_CREDENTIAL() throws Exception {
        appendText("testCloudCredentialString", customFile);
        initProperty("CLOUD_CREDENTIAL", customFile.getAbsolutePath());

        assertEquals("testCloudCredentialString", simulatorProperties.get("CLOUD_CREDENTIAL"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testGet_CLOUD_CREDENTIAL_notFound() throws Exception {
        initProperty("CLOUD_CREDENTIAL", "notFound");

        simulatorProperties.get("CLOUD_CREDENTIAL");
    }

    private void initProperty(String key, Object value) {
        appendText(key + "=" + value, workingDirFile);
        simulatorProperties.init(null);
    }
}
