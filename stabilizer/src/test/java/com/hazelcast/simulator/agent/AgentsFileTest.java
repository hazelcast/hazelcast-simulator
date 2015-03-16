package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static org.junit.Assert.assertEquals;

public class AgentsFileTest {

    @Test
    public void load_publicAndPrivateIp() throws IOException {
        File file = File.createTempFile("AgentsFileTest", "txt");
        file.deleteOnExit();
        writeText("192.168.1.1,10.10.10.10", file);

        List<AgentAddress> addressList = AgentsFile.load(file);
        assertEquals(1, addressList.size());
        assertEquals(new AgentAddress("192.168.1.1", "10.10.10.10"), addressList.get(0));
    }

    @Test
    public void load_onlyPublicIp() throws IOException {
        File file = File.createTempFile("AgentsFileTest", "txt");
        file.deleteOnExit();
        writeText("192.168.1.1", file);

        List<AgentAddress> addressList = AgentsFile.load(file);
        assertEquals(1, addressList.size());
        assertEquals(new AgentAddress("192.168.1.1", "192.168.1.1"), addressList.get(0));
    }

    @Test
    public void load_fileContainsEmptyLines() throws IOException {
        File file = File.createTempFile("AgentsFileTest", "txt");
        file.deleteOnExit();
        writeText("\n192.168.1.1\n\n", file);

        List<AgentAddress> addressList = AgentsFile.load(file);
        assertEquals(1, addressList.size());
        assertEquals(new AgentAddress("192.168.1.1", "192.168.1.1"), addressList.get(0));
    }

    @Test
    public void load_fileContainsComments() throws IOException {
        File file = File.createTempFile("AgentsFileTest", "txt");
        file.deleteOnExit();
        writeText("192.168.1.1#foo\n#bar", file);

        List<AgentAddress> addressList = AgentsFile.load(file);
        assertEquals(1, addressList.size());
        assertEquals(new AgentAddress("192.168.1.1", "192.168.1.1"), addressList.get(0));
    }
}
