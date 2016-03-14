package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.CloudProviderUtils.isLocal;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WizardTest {

    private SimulatorProperties simulatorProperties;

    private File workDir;
    private File testPropertiesFile;
    private File runScriptFile;
    private File prepareScriptFile;
    private File simulatorPropertiesFile;
    private File agentsFile;

    private File profileFile;

    private Wizard wizard;

    @BeforeClass
    public static void setUpEnvironment() {
        setDistributionUserDir();

        setExitExceptionSecurityManagerWithStatusZero();
    }

    @AfterClass
    public static void resetEnvironment() {
        resetUserDir();

        resetSecurityManager();
    }

    @Before
    public void setUp() {
        simulatorProperties = mock(SimulatorProperties.class);

        workDir = new File("wizardTestWorkDir").getAbsoluteFile();
        testPropertiesFile = new File(workDir, "test.properties");
        runScriptFile = new File(workDir, "run");
        prepareScriptFile = new File(workDir, "prepare");
        simulatorPropertiesFile = new File(workDir, SimulatorProperties.PROPERTIES_FILE_NAME);
        agentsFile = new File(workDir, AgentsFile.NAME);

        profileFile = ensureExistingFile("wizardTest.txt");

        wizard = new Wizard(simulatorProperties);
    }

    @After
    public void tearDown() {
        deleteQuiet(profileFile);
        deleteQuiet(workDir);

        deleteQuiet(Wizard.AGENTS_FILE);
        deleteQuiet(Wizard.SSH_COPY_ID_FILE);
    }

    @Test
    public void testMain() {
        Wizard.main(new String[]{"--createWorkDir", workDir.getName()});
    }

    @Test(expected = ExitStatusOneException.class)
    public void testMain_withException() {
        Wizard.main(new String[]{});
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
        wizard.createWorkDir(workDir.getName(), PROVIDER_LOCAL);

        assertCreateWorkDir(PROVIDER_LOCAL);
    }

    @Test
    public void testCreateWorkDir_withCloudProviderStatic() {
        wizard.createWorkDir(workDir.getName(), PROVIDER_STATIC);

        assertCreateWorkDir(PROVIDER_STATIC);
    }

    @Test
    public void testCreateWorkDir_withCloudProviderEC2() {
        wizard.createWorkDir(workDir.getName(), PROVIDER_EC2);

        assertCreateWorkDir(PROVIDER_EC2);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateWorkDir_workDirExists() {
        ensureExistingDirectory(workDir);

        wizard.createWorkDir(workDir.getName(), PROVIDER_LOCAL);
    }

    @Test
    public void testListCloudProviders() {
        wizard.listCloudProviders();
    }

    @Test
    public void testCreateSshCopyIdScript() {
        when(simulatorProperties.getUser()).thenReturn("wizardTestUser");

        appendText("172.16.16.1" + NEW_LINE, Wizard.AGENTS_FILE);
        appendText("172.16.16.2" + NEW_LINE, Wizard.AGENTS_FILE);

        wizard.createSshCopyIdScript();

        assertTrue(Wizard.SSH_COPY_ID_FILE.exists());
        assertTrue(Wizard.SSH_COPY_ID_FILE.isFile());

        String sshCopyIdScript = fileAsText(Wizard.SSH_COPY_ID_FILE);
        assertTrue(sshCopyIdScript.contains("wizardTestUser@172.16.16.1"));
        assertTrue(sshCopyIdScript.contains("wizardTestUser@172.16.16.2"));
    }

    @Test(expected = CommandLineExitException.class)
    public void testSshCopyId_withEmptyAgentsFile() {
        wizard.createSshCopyIdScript();
    }

    private void assertCreateWorkDir(String cloudProvider) {
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
        assertTrue(fileAsText(simulatorPropertiesFile).contains(cloudProvider));

        assertTrue(agentsFile.exists());
        assertTrue(agentsFile.isFile());
    }
}
