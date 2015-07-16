package com.hazelcast.simulator.worker;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.simulator.common.messaging.DummyRunnableMessage;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.exceptions.verification.WantedButNotInvoked;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.utils.TestUtils.VERIFY_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
    public void testSubmit_local() {
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
    public void testSubmit_toAllTests() throws Exception {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toAllTests().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);

        assertFalse(message.isExecuted());
        verify(testContainerMock1, timeout(VERIFY_TIMEOUT_MILLIS)).sendMessage(message);
        verify(testContainerMock2, timeout(VERIFY_TIMEOUT_MILLIS)).sendMessage(message);
    }

    @Test
    public void testSubmit_toRandomAgent() throws Exception {
        MessageAddress messageAddress = MessageAddress.builder().toRandomAgent().toAllWorkers().toAllTests().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        verifyMessageSentToEitherOr(testContainerMock1, testContainerMock2, message);
    }

    @Test
    public void testSubmit_toRandomWorker() throws Exception {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toRandomWorker().toAllTests().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        verifyMessageSentToEitherOr(testContainerMock1, testContainerMock2, message);
    }

    @Test
    public void testSubmit_toRandomTest() throws Exception {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toRandomTest().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        verifyMessageSentToEitherOr(testContainerMock1, testContainerMock2, message);
    }

    @Test
    public void testSubmit_toOldestMember() throws Exception {
        workerMessageProcessor.setHazelcastServerInstance(createMockHazelcastInstance(true));

        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toOldestMember().toAllTests().build();
        final DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verifyMessageSentToEitherOr(testContainerMock1, testContainerMock2, message);
            }
        });
    }

    @Test
    public void testSubmit_toRandomTest_withHazelcastClient() throws Exception {
        workerMessageProcessor.setHazelcastClientInstance(createMockHazelcastInstance(true));

        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toRandomTest().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        verifyMessageSentToEitherOr(testContainerMock1, testContainerMock2, message);
    }

    @Test
    public void testSubmit_noTestContainers() {
        workerMessageProcessor = new WorkerMessageProcessor(new ConcurrentHashMap<String, TestContainer<TestContext>>());

        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toRandomTest().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
        verifyZeroInteractions(testContainerMock1);
        verifyZeroInteractions(testContainerMock2);
        assertFalse(message.isExecuted());
    }

    @Test()
    public void testSubmit_noRunnable() {
        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().build();
        Message noRunnableMessage = mock(Message.class);
        when(noRunnableMessage.getMessageAddress()).thenReturn(messageAddress);

        workerMessageProcessor.submit(noRunnableMessage);
    }

    @Test
    public void testSubmit_withException() throws Exception {
        doThrow(new IllegalArgumentException()).when(testContainerMock1).sendMessage(any(Message.class));

        ConcurrentMap<String, TestContainer<TestContext>> tests = new ConcurrentHashMap<String, TestContainer<TestContext>>();
        tests.put("mockTest", testContainerMock1);

        workerMessageProcessor = new WorkerMessageProcessor(tests);

        MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toRandomTest().build();
        DummyRunnableMessage message = new DummyRunnableMessage(messageAddress);

        workerMessageProcessor.submit(message);
    }

    private void verifyMessageSentToEitherOr(TestContainer<?> container1, TestContainer<?> container2, Message message)
            throws Exception {
        try {
            verify(container1, timeout(VERIFY_TIMEOUT_MILLIS)).sendMessage(message);
        } catch (WantedButNotInvoked e) {
            // the message was not deliver to container1, so it should go to container2
            verify(container2).sendMessage(message);
        }
    }

    private HazelcastInstance createMockHazelcastInstance(boolean returnMember) {
        Member member = mock(Member.class);

        Set<Member> memberSet = new HashSet<Member>();
        memberSet.add(member);

        Cluster cluster = mock(Cluster.class);
        when(cluster.getMembers()).thenReturn(memberSet);

        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        when(hazelcastInstance.getLocalEndpoint()).thenReturn(returnMember ? member : null);
        when(hazelcastInstance.getCluster()).thenReturn(cluster);

        return hazelcastInstance;
    }
}
