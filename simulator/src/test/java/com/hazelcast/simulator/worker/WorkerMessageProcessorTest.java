package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.common.messaging.DummyRunnableMessage;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.exceptions.verification.WantedButNotInvoked;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.utils.TestUtils.TIMEOUT;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class WorkerMessageProcessorTest {

    private WorkerMessageProcessor workerMessageProcessor;
    private TestContainer<TestContext> testContainerMock1;
    private TestContainer<TestContext> testContainerMock2;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        testContainerMock1 = mock(TestContainer.class);
        testContainerMock2 = mock(TestContainer.class);
        ConcurrentMap<String, TestContainer<TestContext>> tests = new ConcurrentHashMap<String, TestContainer<TestContext>>();
        tests.put("mockTest1", testContainerMock1);
        tests.put("mockTest2", testContainerMock2);
        workerMessageProcessor = new WorkerMessageProcessor(tests);
    }

    @Test
    public void testRunnableLocalMessage() {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().build();
        final DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(message.isExecuted());
            }
        });
    }

    @Test
    public void testMessageToAllTests() throws Throwable {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toAllTests().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);

        assertFalse(message.isExecuted());
        verify(testContainerMock1, timeout(TIMEOUT)).sendMessage(message);
        verify(testContainerMock2, timeout(TIMEOUT)).sendMessage(message);
    }

    @Test
    public void testMessageToRandomTest() throws Throwable {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toRandomTest().build();
        final DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        verifyMessageSentToEitherOr(testContainerMock1, testContainerMock2, message);
    }

    private void verifyMessageSentToEitherOr(TestContainer<?> container1, TestContainer<?> container2, Message message)
            throws Throwable {
        try {
            verify(container1, timeout(TIMEOUT)).sendMessage(message);
        } catch (WantedButNotInvoked e) {
            //the message was not deliver to container1, so it should go to container2
            verify(container2).sendMessage(message);
        }
    }
}
