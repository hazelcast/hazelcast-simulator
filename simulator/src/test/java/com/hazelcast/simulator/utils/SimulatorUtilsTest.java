package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;

public class SimulatorUtilsTest {

    private File agentsFile;
    private ComponentRegistry componentRegistry;

    @Before
    public void setUp() throws IOException {
        agentsFile = createTempFile("AgentsFileTest", "txt");
    }

    @After
    public void tearDown() {
        deleteQuiet(agentsFile);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(SimulatorUtils.class);
    }

    @Test
    public void testLoadComponentRegister() {
        writeText("192.168.1.1,10.10.10.10", agentsFile);

        componentRegistry = loadComponentRegister(agentsFile);
        assertEquals(1, componentRegistry.agentCount());
    }

    @Test(expected = CommandLineExitException.class)
    public void testLoadComponentRegister_emptyFile_withSizeCheck() {
        componentRegistry = loadComponentRegister(agentsFile, true);
    }

    @Test
    public void testLoadComponentRegister_emptyFile_withoutSizeCheck() {
        componentRegistry = loadComponentRegister(agentsFile, false);
        assertEquals(0, componentRegistry.agentCount());
    }
}
