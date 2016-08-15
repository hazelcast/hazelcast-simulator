package com.hazelcast.simulator.remotecontroller;

import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RemoteControllerCliTest {

    private File propertiesFile;

    private final List<String> args = new ArrayList<String>();

    private RemoteController remoteController = mock(RemoteController.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        setExitExceptionSecurityManagerWithStatusZero();
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
    }

    @Before
    public void before() {
        setupFakeEnvironment();

        propertiesFile = ensureExistingFile(getUserDir(), "simulator.properties");
        appendText("COORDINATOR_PORT=5555", propertiesFile);
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    // todo: test only causes coverage; doesn't test behavior
    @Test
    public void testInit() {
        new RemoteControllerCli(getArgs());
    }

    // todo: test only causes coverage; doesn't test behavior
    @Test
    public void testInit_whenQuiet() {
        args.add("--quiet");
        new RemoteControllerCli(getArgs());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_whenCoordinatorPortIsDisabled() {
        File simulatorProperties = new File(getUserDir(), "explicit-simulator.properties");
        ensureExistingFile(simulatorProperties);

        args.add("--propertiesFile");
        args.add(simulatorProperties.getAbsolutePath());

        new RemoteControllerCli(getArgs());
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        RemoteControllerCli cli = new RemoteControllerCli(getArgs());
        cli.setRemoteController(remoteController);
        cli.run();
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        RemoteControllerCli cli = new RemoteControllerCli(getArgs());
        cli.setRemoteController(remoteController);
        cli.run();
    }

    @Test
    public void testRun_listComponents() {
        args.add("--listComponents");

        RemoteControllerCli cli = new RemoteControllerCli(getArgs());
        cli.setRemoteController(remoteController);
        cli.run();

        verify(remoteController).start();
        verify(remoteController).listComponents();
        verify(remoteController).shutdown();
        verifyNoMoreInteractions(remoteController);
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
