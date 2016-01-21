package com.hazelcast.simulator.wizard;

import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.wizard.WizardCli.run;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class WizardCliTest {

    private static File bashrc;
    private static boolean deleteFile;

    private final List<String> args = new ArrayList<String>();

    private Wizard wizard = mock(Wizard.class);

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManagerWithStatusZero();
        setDistributionUserDir();

        bashrc = new File(".bashrc").getAbsoluteFile();
        deleteFile = (!bashrc.exists());
        ensureExistingFile(bashrc);
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
        resetUserDir();

        if (deleteFile) {
            deleteQuiet(bashrc);
        }
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(WizardCli.class);
    }

    @Test
    public void testInit() {
        wizard = WizardCli.init();

        assertNotNull(wizard);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        run(getArgs(), wizard);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        run(getArgs(), wizard);
    }

    @Test
    public void testRun_install() {
        args.add("--install");

        run(getArgs(), wizard);

        verify(wizard).install(anyString(), any(File.class));
        verifyNoMoreInteractions(wizard);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
