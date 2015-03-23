package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.messaging.DummyRunnableMessage;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.CommonUtils.getHostAddress;
import static org.mockito.Mockito.*;

@Ignore
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
