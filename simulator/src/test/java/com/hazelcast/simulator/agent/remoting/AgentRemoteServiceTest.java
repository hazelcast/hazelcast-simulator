package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.messaging.DummyRunnableMessage;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AgentRemoteServiceTest {

    private static final String AGENT_ADDRESS = "127.0.0.1";

    private Agent agent;
    private AgentMessageProcessor agentMessageProcessor;
    private AgentRemoteService agentRemoteService;
    private AgentsClient client;

    @Before
    public void setUp() throws IOException {
        WorkerJvmManager workerJvmManager = mock(WorkerJvmManager.class);

        agent = mock(Agent.class);
        when(agent.getWorkerJvmManager()).thenReturn(workerJvmManager);

        agentMessageProcessor = mock(AgentMessageProcessor.class);

        agentRemoteService = new AgentRemoteService(agent, agentMessageProcessor);
        agentRemoteService.start();

        List<AgentAddress> addresses = singletonList(new AgentAddress(AGENT_ADDRESS, AGENT_ADDRESS));
        client = new AgentsClient(addresses);
    }

    @After
    public void tearDown() throws Exception {
        client.stop();
        agentRemoteService.stop();
    }

    @Test
    public void testEcho() throws IOException, TimeoutException {
        String string = "foo";
        client.echo(string);

        verify(agent).echo(string);
    }

    @Test
    public void testMessageToAgent() throws TimeoutException {
        MessageAddress address = MessageAddress.builder().toRandomAgent().build();
        Message message = new DummyRunnableMessage(address);
        client.sendMessage(message);

        verify(agentMessageProcessor).submit(any(Message.class));
    }
}
