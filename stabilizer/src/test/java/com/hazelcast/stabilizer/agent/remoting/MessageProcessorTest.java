package com.hazelcast.stabilizer.agent.remoting;

import com.hazelcast.stabilizer.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class MessageProcessorTest {

    private MessageProcessor messageProcessor;
    private WorkerJvmManager workerJvmManagerMock;

    @Before
    public void setUp() {
        workerJvmManagerMock = mock(WorkerJvmManager.class);
        messageProcessor = new MessageProcessor(workerJvmManagerMock);
    }

    @Test
    public void testLocalMessage() throws TimeoutException, InterruptedException {
        MessageAddress address = MessageAddress.builder().toAllAgents().build();
        DummyRunnableMessage message = new DummyRunnableMessage(address);

        messageProcessor.process(message);
        assertTrue(message.isExecuted());
        verify(workerJvmManagerMock, never()).sendMessage(any(Message.class));
    }

    @Test
    public void testWorkerMessage() throws TimeoutException, InterruptedException {
        MessageAddress address = MessageAddress.builder().toAllAgents().toAllWorkers().build();
        DummyRunnableMessage message = new DummyRunnableMessage(address);
        messageProcessor.process(message);

        assertFalse(message.isExecuted());
        verify(workerJvmManagerMock).sendMessage(message);
    }
}
