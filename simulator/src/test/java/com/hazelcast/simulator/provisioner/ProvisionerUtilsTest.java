package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.provisioner.ProvisionerUtils.INIT_SH_SCRIPT_NAME;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsCloudProviderSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsRemoteSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.getInitScriptFile;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProvisionerUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ProvisionerUtils.class);
    }

    @Test
    public void testGetInitScriptFile() {
        File initScriptFile = ensureExistingFile(INIT_SH_SCRIPT_NAME);
        try {
            File actualInitScriptFile = getInitScriptFile(null);
            assertEquals(initScriptFile, actualInitScriptFile);
        } finally {
            deleteQuiet(initScriptFile);
        }
    }

    @Test
    public void testGetInitScriptFile_loadFromSimulatorHome() {
        boolean deleteFiles = false;
        File directory = new File(getSimulatorHome(), "conf/");
        File initScriptFile = new File(getSimulatorHome(), "conf/" + INIT_SH_SCRIPT_NAME);
        try {
            if (!initScriptFile.exists()) {
                deleteFiles = true;
                ensureExistingDirectory(directory);
                ensureExistingFile(initScriptFile);
            }

            File actualInitScriptFile = getInitScriptFile(getSimulatorHome().getAbsolutePath());
            assertEquals(initScriptFile.getAbsolutePath(), actualInitScriptFile.getAbsolutePath());
        } finally {
            if (deleteFiles) {
                deleteQuiet(initScriptFile);
                deleteQuiet(directory);
            }
        }
    }

    @Test(expected = CommandLineExitException.class)
    public void testGetInitScriptFile_notExists() {
        getInitScriptFile(".");
    }

    @Test
    public void testEnsureIsRemoteSetup_withEC2() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(CloudProviderUtils.PROVIDER_EC2);

        ensureIsRemoteSetup(properties, "terminate");
    }

    @Test(expected = CommandLineExitException.class)
    public void testEnsureIsRemoteSetup_withLocal() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(CloudProviderUtils.PROVIDER_LOCAL);

        ensureIsRemoteSetup(properties, "terminate");
    }

    @Test
    public void testEnsureIsCloudProviderSetup_withEC2() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(CloudProviderUtils.PROVIDER_EC2);

        ensureIsCloudProviderSetup(properties, "terminate");
    }

    @Test(expected = CommandLineExitException.class)
    public void testEnsureIsCloudProviderSetup_withLocal() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(CloudProviderUtils.PROVIDER_LOCAL);

        ensureIsCloudProviderSetup(properties, "terminate");
    }

    @Test(expected = CommandLineExitException.class)
    public void testEnsureIsCloudProviderSetup_withStatic() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getCloudProvider()).thenReturn(CloudProviderUtils.PROVIDER_STATIC);

        ensureIsCloudProviderSetup(properties, "terminate");
    }
}
