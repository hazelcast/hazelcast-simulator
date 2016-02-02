package com.hazelcast.simulator.common;

import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.simulator.common.AgentsFile.load;
import static com.hazelcast.simulator.common.AgentsFile.save;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class AgentsFileTest {

    private File agentsFile;
    private ComponentRegistry componentRegistry;

    @Before
    public void setUp() throws IOException {
        agentsFile = ensureExistingFile("AgentsFileTest-agents.txt");
    }

    @After
    public void tearDown() {
        deleteQuiet(agentsFile);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AgentsFile.class);
    }

    @Test
    public void testLoad_publicAndPrivateAddress() {
        writeText("192.168.1.1,10.10.10.10", agentsFile);

        componentRegistry = load(agentsFile);
        assertEquals(1, componentRegistry.agentCount());

        AgentData agentData = componentRegistry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("10.10.10.10", agentData.getPrivateAddress());
    }

    @Test
    public void testLoad_onlyPublicAddress() {
        writeText("192.168.1.1", agentsFile);

        componentRegistry = load(agentsFile);
        assertEquals(1, componentRegistry.agentCount());

        AgentData agentData = componentRegistry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("192.168.1.1", agentData.getPrivateAddress());
    }

    @Test
    public void testLoad_fileContainsEmptyLines() {
        writeText(NEW_LINE + "192.168.1.1" + NEW_LINE + NEW_LINE, agentsFile);

        componentRegistry = load(agentsFile);
        assertEquals(1, componentRegistry.agentCount());

        AgentData agentData = componentRegistry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("192.168.1.1", agentData.getPrivateAddress());
    }

    @Test
    public void testLoad_fileContainsComments() {
        writeText("192.168.1.1#foo" + NEW_LINE + "#bar", agentsFile);

        componentRegistry = load(agentsFile);
        assertEquals(1, componentRegistry.agentCount());

        AgentData agentData = componentRegistry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("192.168.1.1", agentData.getPrivateAddress());
    }

    @Test(expected = CommandLineExitException.class)
    public void testLoad_fileContainsInvalidAddressLine() {
        writeText("192.168.1.1,192.168.1.1,192.168.1.1", agentsFile);

        componentRegistry = load(agentsFile);
    }

    @Test
    public void testSave() {
        componentRegistry = load(agentsFile);

        componentRegistry.addAgent("192.168.1.1", "192.168.1.1");
        componentRegistry.addAgent("192.168.1.1", "10.10.10.10");
        assertEquals(2, componentRegistry.agentCount());

        save(agentsFile, componentRegistry);

        ComponentRegistry actualRegistry = load(agentsFile);
        assertEquals(2, actualRegistry.agentCount());

        List<AgentData> agents = actualRegistry.getAgents();
        assertEquals("192.168.1.1", agents.get(0).getPublicAddress());
        assertEquals("192.168.1.1", agents.get(0).getPrivateAddress());
        assertEquals("192.168.1.1", agents.get(1).getPublicAddress());
        assertEquals("10.10.10.10", agents.get(1).getPrivateAddress());
    }
}
