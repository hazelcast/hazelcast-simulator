package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static org.junit.Assert.assertTrue;

public class WizardTest {

    private File profileFile;
    private Wizard wizard;

    @Before
    public void setUp() {
        setDistributionUserDir();

        profileFile = ensureExistingFile("wizardTest.txt");
        wizard = new Wizard();
    }

    @After
    public void tearDown() {
        resetUserDir();

        deleteQuiet(profileFile);
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
}
