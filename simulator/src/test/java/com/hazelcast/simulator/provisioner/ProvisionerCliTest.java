package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.createCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.createPublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteCloudCredentialFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.deletePublicPrivateKeyFiles;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static java.util.Collections.EMPTY_MAP;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProvisionerCliTest {

    private final List<String> args = new ArrayList<String>();

    private Provisioner provisioner = mock(Provisioner.class);

    @BeforeClass
    public static void beforeClass() {
        setExitExceptionSecurityManagerWithStatusZero();
        setupFakeEnvironment();

        createAgentsFileWithLocalhost();
        createCloudCredentialFiles();
        createPublicPrivateKeyFiles();
    }

    @AfterClass
    public static void afterClass() {
        resetSecurityManager();
        tearDownFakeEnvironment();
        deleteCloudCredentialFiles();
        deletePublicPrivateKeyFiles();
    }

    @Test
    public void testInit() {
        ProvisionerCli cli = new ProvisionerCli(getArgs());

        Registry registry = cli.getProvisioner().getRegistry();
        assertEquals(1, registry.agentCount());
        assertEquals("127.0.0.1", registry.getFirstAgent().getPublicAddress());
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

        verify(provisioner).scale(0, new HashMap<String, String>());
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

        verify(provisioner).scale(10, new HashMap<String, String>());
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
    public void testRun_kill() {
        args.add("--kill");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).killJavaProcesses(false);
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    @Test
    public void testRun_sudokill() {
        args.add("--sudokill");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).killJavaProcesses(true);
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

    @Test
    public void testRun_ansible() {
        args.add("--ansible");
        args.add("hadoop.yml");

        ProvisionerCli cli = new ProvisionerCli(getArgs());
        cli.setProvisioner(provisioner);
        cli.run();

        verify(provisioner).ansibleScript("hadoop.yml", EMPTY_MAP);
        verify(provisioner).shutdown();
        verifyNoMoreInteractions(provisioner);
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
