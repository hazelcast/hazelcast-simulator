package com.hazelcast.simulator.wizard;

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
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static org.junit.Assert.assertTrue;

public class WizardTest {

    private File workDir;
    private File runScriptFile;
    private File testPropertiesFile;
    private File simulatorPropertiesFile;

    private File profileFile;
    private Wizard wizard;

    @BeforeClass
    public static void setUpSecurityManager() {
        setExitExceptionSecurityManagerWithStatusZero();
    }

    @AfterClass
    public static void tearDownSecurityManager() {
        resetSecurityManager();
    }

    @Before
    public void setUp() {
        setDistributionUserDir();

        workDir = new File("wizardTestWorkDir").getAbsoluteFile();
        runScriptFile = new File(workDir, "run");
        testPropertiesFile = new File(workDir, "test.properties");
        simulatorPropertiesFile = new File(workDir, "simulator.properties");

        profileFile = ensureExistingFile("wizardTest.txt");
        wizard = new Wizard();
    }

    @After
    public void tearDown() {
        resetUserDir();

        deleteQuiet(profileFile);
        deleteQuiet(workDir);
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
    public void testCreateWorkDir() {
        wizard.createWorkDir(workDir.getName(), PROVIDER_EC2);

        assertTrue(workDir.exists());
        assertTrue(workDir.isDirectory());

        assertTrue(runScriptFile.exists());
        assertTrue(runScriptFile.isFile());

        assertTrue(testPropertiesFile.exists());
        assertTrue(testPropertiesFile.isFile());

        assertTrue(simulatorPropertiesFile.exists());
        assertTrue(simulatorPropertiesFile.isFile());
        assertTrue(fileAsText(simulatorPropertiesFile).contains(PROVIDER_EC2));
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
}
