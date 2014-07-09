package com.hazelcast.stabilizer.agent;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.coordinator.AgentsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;


public class AgentRemoteServiceTest {

    private Agent agentMock;
    private AgentRemoteService agentRemoteService;
    private AgentsClient client;

    @Before
    public void setUp() throws IOException {
        agentMock = mock(Agent.class);
        agentRemoteService = new AgentRemoteService(agentMock);
        agentRemoteService.start();
        InetAddress localInetAddress = InetAddress.getByName(Utils.getHostAddress());
        String addressString = localInetAddress.getHostAddress();
        List<AgentAddress> addresses = Arrays.asList(new AgentAddress(addressString, addressString));
        client = new AgentsClient(addresses);
    }

    @After
    public void tearDown() throws IOException {
        agentRemoteService.stop();
    }


    @Test
    public void testEcho() throws IOException {
        String string = "foo";

        client.echo(string);
        verify(agentMock).echo(string);
    }

}
