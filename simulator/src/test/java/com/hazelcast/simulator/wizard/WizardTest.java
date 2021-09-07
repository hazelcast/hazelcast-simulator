package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class WizardTest {

    private static final String SSH_USERNAME = "wizardTestUser";

    private SimulatorProperties simulatorProperties;
    private Bash bash;

    private File workDir;
    private File testPropertiesFile;
    private File runScriptFile;
    private File prepareScriptFile;
    private File simulatorPropertiesFile;
    private File agentsFile;

    private File localSimulatorPropertiesFile;
    private File profileFile;

    private Wizard wizard;

    @BeforeClass
    public static  void beforeClass() {
        setExitExceptionSecurityManagerWithStatusZero();
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
    }

    @Before
    public void before() {
        setupFakeEnvironment();

        simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.getUser()).thenReturn(SSH_USERNAME);
        when(simulatorProperties.getSshOptions()).thenReturn("");

        bash = mock(Bash.class);

        workDir = new File("wizardTestWorkDir").getAbsoluteFile();
        testPropertiesFile = new File(workDir, "test.properties");
        runScriptFile = new File(workDir, "run");
        prepareScriptFile = new File(workDir, "prepare");
        simulatorPropertiesFile = new File(workDir, SimulatorProperties.PROPERTIES_FILE_NAME);
        agentsFile = new File(workDir, AgentsFile.NAME);

        localSimulatorPropertiesFile = new File(SimulatorProperties.PROPERTIES_FILE_NAME).getAbsoluteFile();
        profileFile = ensureExistingFile("wizardTest.txt");

        wizard = new Wizard();
    }

    @After
    public void after() {
        tearDownFakeEnvironment();

        deleteQuiet(localSimulatorPropertiesFile);
        deleteQuiet(profileFile);
        deleteQuiet(workDir);

        deleteQuiet(Wizard.SSH_COPY_ID_FILE);
    }

    @Test
    public void testInstallSimulator() {
        wizard.install(".", profileFile);

        String profile = fileAsText(profileFile);
        assertTrue(profile.contains("SIMULATOR_HOME"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testInstallSimulator_installAlreadyDone() {
        appendText("SIMULATOR_HOME=/tmp", profileFile);

        wizard.install(".", profileFile);
    }

    @Test
    public void testCreateWorkDir_withCloudProviderLocal() {
        wizard.createWorkDir(simulatorProperties, workDir.getName(), PROVIDER_LOCAL);

        assertThatWorkingDirFilesHaveBeenCreated(PROVIDER_LOCAL);
    }

    @Test
    public void testCreateWorkDir_withCloudProviderStatic() {
        wizard.createWorkDir(simulatorProperties, workDir.getName(), PROVIDER_STATIC);

        assertThatWorkingDirFilesHaveBeenCreated(PROVIDER_STATIC);
    }

    @Test
    public void testCreateWorkDir_withCloudProviderEC2() {
        wizard.createWorkDir(simulatorProperties, workDir.getName(), PROVIDER_EC2);

        assertThatWorkingDirFilesHaveBeenCreated(PROVIDER_EC2);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkDir_workDirExists() {
        ensureExistingDirectory(workDir);

        wizard.createWorkDir(simulatorProperties, workDir.getName(), PROVIDER_LOCAL);
    }

    @Test
    public void testListCloudProviders() {
        wizard.listCloudProviders();
    }

    @Test
    public void testCreateSshCopyIdScript() {
        addIpAddressToAgentsFile("172.16.16.1");
        addIpAddressToAgentsFile("172.16.16.2");

        wizard.createSshCopyIdScript(simulatorProperties);

        assertTrue(Wizard.SSH_COPY_ID_FILE.exists());
        assertTrue(Wizard.SSH_COPY_ID_FILE.isFile());

        String sshCopyIdScript = fileAsText(Wizard.SSH_COPY_ID_FILE);
        assertTrue(sshCopyIdScript.contains(SSH_USERNAME + "@172.16.16.1"));
        assertTrue(sshCopyIdScript.contains(SSH_USERNAME + "@172.16.16.2"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testSshCopyId_withEmptyAgentsFile() {
        wizard.createSshCopyIdScript(simulatorProperties);
    }

    @Test
    public void testSshConnectionTest() {
        when(simulatorProperties.getCloudProvider()).thenReturn(PROVIDER_STATIC);

        addIpAddressToAgentsFile("172.16.16.1");
        addIpAddressToAgentsFile("172.16.16.2");

        wizard.sshConnectionCheck(simulatorProperties, bash);

        verify(bash).ssh(eq("172.16.16.1"), anyString());
        verify(bash).ssh(eq("172.16.16.2"), anyString());
        verifyNoMoreInteractions(bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSshConnectionTest_withCloudProviderLocal() {
        when(simulatorProperties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);

        wizard.sshConnectionCheck(simulatorProperties, bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testSshConnectionTest_withEmptyAgentsFile() {
        when(simulatorProperties.getCloudProvider()).thenReturn(PROVIDER_EC2);

        wizard.sshConnectionCheck(simulatorProperties, bash);
    }

    @Test
    public void testCompareSimulatorProperties() {
        SimulatorProperties defaultProperties = new SimulatorProperties();
        ensureExistingFile(localSimulatorPropertiesFile);

        appendText("invalid=unknown" + NEW_LINE, localSimulatorPropertiesFile);
        appendText(format("%s=changed%n", CLOUD_PROVIDER), localSimulatorPropertiesFile);

        wizard.compareSimulatorProperties();
    }

    @Test
    public void testCompareSimulatorProperties_noPropertiesDefined() {
        wizard.compareSimulatorProperties();
    }

    private void addIpAddressToAgentsFile(String ipAddress) {
        appendText(ipAddress + NEW_LINE, new File(getUserDir(), "agents.txt"));
    }

    private void assertThatWorkingDirFilesHaveBeenCreated(String cloudProvider) {
        assertTrue(workDir.exists());
        assertTrue(workDir.isDirectory());

        assertTrue(testPropertiesFile.exists());
        assertTrue(testPropertiesFile.isFile());

        assertTrue(runScriptFile.exists());
        assertTrue(runScriptFile.isFile());

        if (isLocal(cloudProvider)) {
            return;
        }

        assertTrue(prepareScriptFile.exists());
        assertTrue(prepareScriptFile.isFile());

        assertTrue(simulatorPropertiesFile.exists());
        assertTrue(simulatorPropertiesFile.isFile());

        String simulatorPropertiesContent = fileAsText(simulatorPropertiesFile);
        assertTrue(simulatorPropertiesContent.contains(cloudProvider));

        assertTrue(agentsFile.exists());
        assertTrue(agentsFile.isFile());
    }
}
