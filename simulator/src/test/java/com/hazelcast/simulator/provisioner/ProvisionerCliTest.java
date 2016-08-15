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
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deletePublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProvisionerCliTest {

    private final List<String> args = new ArrayList<String>();

    private Provisioner provisioner = mock(Provisioner.class);

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManagerWithStatusZero();
        setupFakeEnvironment();

        createAgentsFileWithLocalhost();
        createCloudCredentialFiles();
        createPublicPrivateKeyFiles();
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
        tearDownFakeEnvironment();
        deleteCloudCredentialFiles();
        deletePublicPrivateKeyFiles();
    }

    @Test
    public void testInit() {
        ProvisionerCli cli = new ProvisionerCli(getArgs());

        ComponentRegistry componentRegistry = cli.getProvisioner().getComponentRegistry();
        assertEquals(1, componentRegistry.agentCount());
        assertEquals("127.0.0.1", componentRegistry.getFirstAgent().getPublicAddress());
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withoutArguments() {
        new ProvisionerCli(getArgs()).run();
    }

    @Test(expected = ExitStatusZeroException.class)
    public void testRun_withHelp() {
        args.add("--help");
        new ProvisionerCli(getArgs());
    }

    @Test
    public void testRun_scaleZero() {
        args.add("--scale");
        args.add("0");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).scale(0);
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_scalePositiveNumber() {
        args.add("--scale");
        args.add("10");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).scale(10);
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_install() {
        args.add("--install");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).installSimulator();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_download_defaultDirectory() {
        args.add("--download");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).download("workers");
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_download_customDirectory() {
        args.add("--download");
        args.add("outputDir");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).download("outputDir");
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_clean() {
        args.add("--clean");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).clean();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_kill() {
        args.add("--kill");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).killJavaProcesses();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_terminate() {
        args.add("--terminate");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).terminate();
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
