package com.hazelcast.simulator.remotecontroller;

import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteAgentsFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.remotecontroller.RemoteControllerCli.run;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RemoteControllerCliTest {

    private static File propertiesFile;

    private final List<String> args = new ArrayList<String>();

    private RemoteController remoteController = mock(RemoteController.class);

    @BeforeClass
    public static void beforeClass() throws Exception {
        setExitExceptionSecurityManagerWithStatusZero();
        setDistributionUserDir();

        propertiesFile = ensureExistingFile("simulator.properties");
        appendText("COORDINATOR_PORT=5555", propertiesFile);
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
        resetUserDir();
        deleteAgentsFile();

        deleteQuiet(propertiesFile);
    }

    @Test
    public void testInit() {
        createRemoteController();
    }

    @Test
    public void testInit_whenQuiet() {
        args.add("--quiet");

        createRemoteController();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_whenCoordinatorPortIsDisabled() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        ensureExistingFile(simulatorProperties);

        try {
            args.add("--propertiesFile");
            args.add(simulatorProperties.getAbsolutePath());

            createRemoteController();
        } finally {
            deleteQuiet(simulatorProperties);
        }
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        run(getArgs(), remoteController);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        run(getArgs(), remoteController);
    }

    @Test
    public void testRun_listComponents() {
        args.add("--listComponents");

        run(getArgs(), remoteController);

        verify(remoteController).start();
        verify(remoteController).listComponents();
        verify(remoteController).shutdown();
        verifyNoMoreInteractions(remoteController);
    }

    private RemoteController createRemoteController() {
        return RemoteControllerCli.init(getArgs());
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
