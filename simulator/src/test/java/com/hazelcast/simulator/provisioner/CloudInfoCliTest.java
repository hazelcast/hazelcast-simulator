package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.createCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.createPublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteAgentsFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.deletePublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.provisioner.CloudInfoCli.init;
import static com.hazelcast.simulator.provisioner.CloudInfoCli.run;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CloudInfoCliTest {

    private final List<String> args = new ArrayList<String>();

    private CloudInfo cloudInfo = mock(CloudInfo.class);

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManagerWithStatusZero();
        setDistributionUserDir();
        createAgentsFileWithLocalhost();
        createCloudCredentialFiles();
        createPublicPrivateKeyFiles();
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
        resetUserDir();
        deleteLogs();
        deleteAgentsFile();
        deleteCloudCredentialFiles();
        deletePublicPrivateKeyFiles();
    }

    @Test
    public void testInit() {
        init(getArgs());
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        run(getArgs(), cloudInfo);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        run(getArgs(), cloudInfo);
    }

    @Test
    public void testRun_showLocations() {
        args.add("--showLocations");

        run(getArgs(), cloudInfo);

        verify(cloudInfo).showLocations();
        verify(cloudInfo).shutdown();
        verifyNoMoreInteractions(cloudInfo);
    }

    @Test
    public void testRun_showHardware() {
        args.add("--showHardware");

        run(getArgs(), cloudInfo);

        verify(cloudInfo).showHardware();
        verify(cloudInfo).shutdown();
        verifyNoMoreInteractions(cloudInfo);
    }

    @Test
    public void testRun_showImages() {
        args.add("--showImages");

        run(getArgs(), cloudInfo);

        verify(cloudInfo).showImages();
        verify(cloudInfo).shutdown();
        verifyNoMoreInteractions(cloudInfo);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
