package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.helper.ExitStatusZeroException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteAgentsFile;
import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManagerWithStatusZero;
import static com.hazelcast.simulator.provisioner.ComputeServiceBuilder.PRIVATE_KEY;
import static com.hazelcast.simulator.provisioner.ComputeServiceBuilder.PUBLIC_KEY;
import static com.hazelcast.simulator.provisioner.ProvisionerCli.init;
import static com.hazelcast.simulator.provisioner.ProvisionerCli.run;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProvisionerCliTest {

    private static boolean deletePublicKey;
    private static boolean deletePrivateKey;

    private final List<String> args = new ArrayList<String>();

    private Provisioner provisioner = mock(Provisioner.class);

    @BeforeClass
    public static void setUp() throws Exception {
        setExitExceptionSecurityManagerWithStatusZero();
        setDistributionUserDir();
        createAgentsFileWithLocalhost();

        if (!PUBLIC_KEY.exists()) {
            deletePublicKey = true;
            ensureExistingFile(PUBLIC_KEY);
        }
        if (!PRIVATE_KEY.exists()) {
            deletePrivateKey = true;
            ensureExistingFile(PRIVATE_KEY);
        }
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
        resetUserDir();
        deleteLogs();
        deleteAgentsFile();

        if (deletePublicKey) {
            deleteQuiet(PUBLIC_KEY);
        }
        if (deletePrivateKey) {
            deleteQuiet(PRIVATE_KEY);
        }
    }

    @Test
    public void testInit() {
        Provisioner provisioner = init(getArgs());
        ComponentRegistry componentRegistry = provisioner.getComponentRegistry();

        assertEquals(1, componentRegistry.agentCount());
        assertEquals("127.0.0.1", componentRegistry.getFirstAgent().getPublicAddress());
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
    public void testRun_list() {
        args.add("--list");

        run(getArgs(), provisioner);

        verify(provisioner).listMachines();
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
