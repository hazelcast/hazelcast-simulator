package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.SimulatorUtils.getPropertiesFile;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulatorUtilsTest {

    private File agentsFile;
    private ComponentRegistry registry;

    @Before
    public void before() throws IOException {
        setupFakeEnvironment();
        agentsFile = ensureExistingFile(getUserDir(), "SimulatorUtilsTest-agentsFile.txt");
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(SimulatorUtils.class);
    }

    @Test
    public void testLoadComponentRegister() {
        writeText("192.168.1.1,10.10.10.10", agentsFile);

        registry = loadComponentRegister(agentsFile);
        assertEquals(1, registry.agentCount());
    }

    @Test(expected = CommandLineExitException.class)
    public void testLoadComponentRegister_emptyFile_withSizeCheck() {
        registry = loadComponentRegister(agentsFile, true);
    }

    @Test
    public void testLoadComponentRegister_emptyFile_withoutSizeCheck() {
        registry = loadComponentRegister(agentsFile, false);
        assertEquals(0, registry.agentCount());
    }

    @Test
    public void testGetPropertiesFile() {
        OptionSet options = mock(OptionSet.class);
        when(options.has(any(OptionSpec.class))).thenReturn(true);
        when(options.valueOf(any(OptionSpec.class))).thenReturn("test");

        File expectedFile = new File("test");
        File actualFile = getPropertiesFile(options, null);

        assertEquals(expectedFile, actualFile);
    }

    @Test
    public void testGetPropertiesFile_noPropertiesSpec() {
        OptionSet options = mock(OptionSet.class);
        when(options.has(any(OptionSpec.class))).thenReturn(false);

        File actualFile = getPropertiesFile(options, null);

        assertEquals(null, actualFile);
    }
}
