package com.hazelcast.stabilizer.agent;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AgentsFileTest {

    @Test
    public void load_fileContainsEmptyLines() throws IOException {
        File file = File.createTempFile("AgentsFileTest", "txt");
        file.deleteOnExit();
        Utils.writeText("\n192.168.1.1#foo\n\n", file);

        List<AgentAddress> addressList = AgentsFile.load(file);
        assertEquals(1, addressList.size());
        assertEquals(new AgentAddress("192.168.1.1", "192.168.1.1"), addressList.get(0));
    }

    @Test
    public void load_fileContainsComments() throws IOException {
        File file = File.createTempFile("AgentsFileTest", "txt");
        file.deleteOnExit();
        Utils.writeText("192.168.1.1#foo\n" +
                "#bar", file);

        List<AgentAddress> addressList = AgentsFile.load(file);
        assertEquals(1, addressList.size());
        assertEquals(new AgentAddress("192.168.1.1", "192.168.1.1"), addressList.get(0));
    }
}
