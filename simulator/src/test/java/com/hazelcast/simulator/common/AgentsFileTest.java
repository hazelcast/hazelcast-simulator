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
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.io.File.createTempFile;
import static org.junit.Assert.assertEquals;

public class AgentsFileTest {

    private File file;
    private ComponentRegistry registry = new ComponentRegistry();

    @Before
    public void setUp() throws IOException {
        file = createTempFile("AgentsFileTest", "txt");
    }

    @After
    public void tearDown() {
        deleteQuiet(file);
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AgentsFile.class);
    }

    @Test
    public void testLoad_publicAndPrivateAddress() {
        writeText("192.168.1.1,10.10.10.10", file);

        load(file, registry);
        assertEquals(1, registry.agentCount());

        AgentData agentData = registry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("10.10.10.10", agentData.getPrivateAddress());
    }

    @Test
    public void testLoad_onlyPublicAddress() {
        writeText("192.168.1.1", file);

        load(file, registry);
        assertEquals(1, registry.agentCount());

        AgentData agentData = registry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("192.168.1.1", agentData.getPrivateAddress());
    }

    @Test
    public void testLoad_fileContainsEmptyLines() {
        writeText("\n192.168.1.1\n\n", file);

        load(file, registry);
        assertEquals(1, registry.agentCount());

        AgentData agentData = registry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("192.168.1.1", agentData.getPrivateAddress());
    }

    @Test
    public void testLoad_fileContainsComments() {
        writeText("192.168.1.1#foo\n#bar", file);

        load(file, registry);
        assertEquals(1, registry.agentCount());

        AgentData agentData = registry.getFirstAgent();
        assertEquals("192.168.1.1", agentData.getPublicAddress());
        assertEquals("192.168.1.1", agentData.getPrivateAddress());
    }

    @Test(expected = CommandLineExitException.class)
    public void testLoad_fileContainsInvalidAddressLine() {
        writeText("192.168.1.1,192.168.1.1,192.168.1.1", file);

        load(file, registry);
    }

    @Test
    public void testSave() {
        registry.addAgent("192.168.1.1", "192.168.1.1");
        registry.addAgent("192.168.1.1", "10.10.10.10");
        assertEquals(2, registry.agentCount());

        save(file, registry);

        ComponentRegistry actualRegistry = new ComponentRegistry();
        load(file, actualRegistry);
        assertEquals(2, actualRegistry.agentCount());

        List<AgentData> agents = actualRegistry.getAgents();
        assertEquals("192.168.1.1", agents.get(0).getPublicAddress());
        assertEquals("192.168.1.1", agents.get(0).getPrivateAddress());
        assertEquals("192.168.1.1", agents.get(1).getPublicAddress());
        assertEquals("10.10.10.10", agents.get(1).getPrivateAddress());
    }
}
