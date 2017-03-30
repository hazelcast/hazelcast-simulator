package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class WizardCliTest {

    private static File bashrc;
    private static boolean deleteFile;

    private final List<String> args = new ArrayList<String>();

    private Wizard wizard = mock(Wizard.class);

    @BeforeClass
    public static void beforeClass() {
        setExitExceptionSecurityManagerWithStatusZero();
        setupFakeEnvironment();

        bashrc = new File(getUserDir(), ".bashrc").getAbsoluteFile();
        deleteFile = !bashrc.exists();
        ensureExistingFile(bashrc);
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
        tearDownFakeEnvironment();

        if (deleteFile) {
            deleteQuiet(bashrc);
        }
    }

    @Test
    public void testInit() {
        wizard = new WizardCli(new String[0]).wizard;

        assertNotNull(wizard);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        WizardCli cli = new WizardCli(getArgs());
        cli.run();

    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");

        WizardCli cli = new WizardCli(getArgs());
        cli.run();
    }

    @Test
    public void testRun_install() {
        args.add("--install");

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).install(anyString(), any(File.class));
        verifyNoMoreInteractions(wizard);
    }

    @Test
    public void testRun_createWorkDir() {
        args.add("--createWorkDir");
        args.add("tests");

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).createWorkDir(any(SimulatorProperties.class), eq("tests"), eq(CloudProviderUtils.PROVIDER_LOCAL));
        verifyNoMoreInteractions(wizard);
    }

    @Test
    public void testRun_createWorkDir_withCloudProvider() {
        args.add("--createWorkDir");
        args.add("tests");
        args.add("--cloudProvider");
        args.add(CloudProviderUtils.PROVIDER_EC2);

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).createWorkDir(any(SimulatorProperties.class), eq("tests"), eq(CloudProviderUtils.PROVIDER_EC2));
        verifyNoMoreInteractions(wizard);
    }

    @Test
    public void testRun_listCloudProviders() {
        args.add("--list");

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).listCloudProviders();
        verifyNoMoreInteractions(wizard);
    }

    @Test
    public void testRun_createSshCopyIdScript() {
        args.add("--createSshCopyIdScript");

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).createSshCopyIdScript(any(SimulatorProperties.class));
        verifyNoMoreInteractions(wizard);
    }

    @Test
    public void testRun_sshConnectionCheck() {
        args.add("--sshConnectionCheck");

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).sshConnectionCheck(any(SimulatorProperties.class), any(Bash.class));
        verifyNoMoreInteractions(wizard);
    }

    @Test
    public void testRun_compareSimulatorProperties() {
        args.add("--compareSimulatorProperties");

        WizardCli cli = new WizardCli(getArgs());
        cli.wizard = wizard;
        cli.run();

        verify(wizard).compareSimulatorProperties();
        verifyNoMoreInteractions(wizard);
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
