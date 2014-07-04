package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.messaging.RunnableMessage;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
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
    private MessageProcessor messageProcessorMock;

    @Before
    public void setUp() throws IOException {
        agentMock = mock(Agent.class);
        messageProcessorMock = mock(MessageProcessor.class);
        agentRemoteService = new AgentRemoteService(agentMock, messageProcessorMock);
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

    @Test
    public void testMessageToAgent() {
        MessageAddress address = MessageAddress.builder().toRandomAgent().build();
        Message message = new DummyRunnableMessage(address);
        client.sendMessage(message);
        verify(messageProcessorMock).process(any(Message.class));
    }


}
