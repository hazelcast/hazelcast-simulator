package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.agent.Agent;
import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.messaging.DummyRunnableMessage;
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
import java.util.concurrent.TimeoutException;

import static com.hazelcast.stabilizer.utils.CommonUtils.getHostAddress;
import static org.mockito.Mockito.*;

public class AgentRemoteServiceTest {

    private Agent agentMock;
    private AgentRemoteService agentRemoteService;
    private AgentsClient client;
    private AgentMessageProcessor agentMessageProcessorMock;

    @Before
    public void setUp() throws IOException {
        agentMock = mock(Agent.class);
        agentMessageProcessorMock = mock(AgentMessageProcessor.class);
        agentRemoteService = new AgentRemoteService(agentMock, agentMessageProcessorMock);
        agentRemoteService.start();
        InetAddress localInetAddress = InetAddress.getByName(getHostAddress());
        String addressString = localInetAddress.getHostAddress();
        List<AgentAddress> addresses = Arrays.asList(new AgentAddress(addressString, addressString));
        client = new AgentsClient(addresses);
    }

    @After
    public void tearDown() throws IOException {
        agentRemoteService.stop();
    }

    @Test
    public void testEcho() throws IOException, TimeoutException {
        String string = "foo";
        client.echo(string);
        verify(agentMock).echo(string);
    }

    @Test
    public void testMessageToAgent() throws TimeoutException {
        MessageAddress address = MessageAddress.builder().toRandomAgent().build();
        Message message = new DummyRunnableMessage(address);
        client.sendMessage(message);
        verify(agentMessageProcessorMock).submit(any(Message.class));
    }
}
