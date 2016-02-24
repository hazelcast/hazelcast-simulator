package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
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
import static com.hazelcast.simulator.provisioner.ProvisionerCli.init;
import static com.hazelcast.simulator.provisioner.ProvisionerCli.run;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProvisionerCliTest {

    private final List<String> args = new ArrayList<String>();

    private Provisioner provisioner = mock(Provisioner.class);

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
        provisioner = init(getArgs());

        ComponentRegistry componentRegistry = provisioner.getComponentRegistry();
        assertEquals(1, componentRegistry.agentCount());
        assertEquals("127.0.0.1", componentRegistry.getFirstAgent().getPublicAddress());
        assertNull(provisioner.getHazelcastJARs());
    }

    @Test
    public void testInit_withHazelcastUpload() {
        args.add("--uploadHazelcast");

        provisioner = init(getArgs());

        ComponentRegistry componentRegistry = provisioner.getComponentRegistry();
        assertEquals(1, componentRegistry.agentCount());
        assertEquals("127.0.0.1", componentRegistry.getFirstAgent().getPublicAddress());
        assertNotNull(provisioner.getHazelcastJARs());
    }

    @Test
    public void testInit_withHazelcastUpload_withEnterpriseEnabled_withOutOfTheBox() {
        args.add("--uploadHazelcast");
        args.add("--enterpriseEnabled");
        args.add("true");

        provisioner = init(getArgs());

        ComponentRegistry componentRegistry = provisioner.getComponentRegistry();
        assertEquals(1, componentRegistry.agentCount());
        assertEquals("127.0.0.1", componentRegistry.getFirstAgent().getPublicAddress());
        assertNull(provisioner.getHazelcastJARs());
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        run(getArgs(), provisioner);
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        run(getArgs(), provisioner);
    }

    @Test
    public void testRun_scaleZero() {
        args.add("--scale");
        args.add("0");

        run(getArgs(), provisioner);

        verify(provisioner).scale(0);
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_scalePositiveNumber() {
        args.add("--scale");
        args.add("10");

        run(getArgs(), provisioner);

        verify(provisioner).scale(10);
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_install() {
        args.add("--install");

        run(getArgs(), provisioner);

        verify(provisioner).installSimulator();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_download_defaultDirectory() {
        args.add("--download");

        run(getArgs(), provisioner);

        verify(provisioner).download("workers");
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_download_customDirectory() {
        args.add("--download");
        args.add("outputDir");

        run(getArgs(), provisioner);

        verify(provisioner).download("outputDir");
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_clean() {
        args.add("--clean");

        run(getArgs(), provisioner);

        verify(provisioner).clean();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_kill() {
        args.add("--kill");

        run(getArgs(), provisioner);

        verify(provisioner).killJavaProcesses();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_terminate() {
        args.add("--terminate");

        run(getArgs(), provisioner);

        verify(provisioner).terminate();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    private String[] getArgs() {
        String[] argsArray = new String[args.size()];
        args.toArray(argsArray);
        return argsArray;
    }
}
