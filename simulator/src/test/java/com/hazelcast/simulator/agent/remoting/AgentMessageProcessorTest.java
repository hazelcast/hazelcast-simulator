package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.AssertTask;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.common.messaging.DummyRunnableMessage;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static com.hazelcast.simulator.TestSupport.*;


public class AgentMessageProcessorTest {

    private AgentMessageProcessor agentMessageProcessor;
    private WorkerJvmManager workerJvmManagerMock;

    @Before
    public void setUp() {
        workerJvmManagerMock = mock(WorkerJvmManager.class);
        agentMessageProcessor = new AgentMessageProcessor(workerJvmManagerMock);
    }

    @Test
    public void testLocalMessage() throws TimeoutException, InterruptedException {
        MessageAddress address = MessageAddress.builder().toAllAgents().build();
        final DummyRunnableMessage message = new DummyRunnableMessage(address);

        agentMessageProcessor.submit(message);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(message.isExecuted());
            }
        });
        verify(workerJvmManagerMock, never()).sendMessage(any(Message.class));
    }

    @Test
    public void testWorkerMessage() throws TimeoutException, InterruptedException {
        MessageAddress address = MessageAddress.builder().toAllAgents().toAllWorkers().build();
        DummyRunnableMessage message = new DummyRunnableMessage(address);
        agentMessageProcessor.submit(message);

        assertFalse(message.isExecuted());
        verify(workerJvmManagerMock, timeout(TIMEOUT)).sendMessage(message);
    }
}
