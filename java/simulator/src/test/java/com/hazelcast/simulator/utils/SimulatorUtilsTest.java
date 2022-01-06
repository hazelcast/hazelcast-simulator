package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.coordinator.registry.Registry;
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
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static org.junit.Assert.assertEquals;

public class SimulatorUtilsTest {

    private File agentsFile;
    private Registry registry;

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

    @Test
    public void testLoadComponentRegister_emptyFile_withoutSizeCheck() {
        registry = loadComponentRegister(agentsFile);
        assertEquals(0, registry.agentCount());
    }

}
