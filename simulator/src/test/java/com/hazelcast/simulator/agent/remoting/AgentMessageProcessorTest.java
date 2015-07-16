package com.hazelcast.simulator.agent.remoting;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.common.messaging.DummyRunnableMessage;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.TestUtils.VERIFY_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

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
        verify(workerJvmManagerMock, timeout(VERIFY_TIMEOUT_MILLIS)).sendMessage(message);
    }
}
