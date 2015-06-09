package com.hazelcast.simulator.tests.topic;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class ReliableTopicTest {

    private static final ILogger LOGGER = Logger.getLogger(ReliableTopicTest.class);

    // properties
    public int topicCount = 10;
    public int threadCount = 3;
    public int listenersPerTopic = 2;
    public String basename = "reliableTopic";

    private AtomicLong failures = new AtomicLong();
    private IAtomicLong totalMessagesSend;
    private ITopic[] topics;
    private TestContext testContext;
    private HazelcastInstance hz;
    private List<MessageListenerImpl> listeners;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        hz = testContext.getTargetInstance();
        totalMessagesSend = hz.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        topics = new ITopic[topicCount];
        listeners = new LinkedList<MessageListenerImpl>();

        int listenerIdCounter = 0;
        for (int k = 0; k < topics.length; k++) {
            ITopic<MessageEntity> topic = hz.getReliableTopic(basename + "-" + k);
            topics[k] = topic;
            for (int l = 0; l < listenersPerTopic; l++) {
                MessageListenerImpl topicListener = new MessageListenerImpl(listenerIdCounter);
                listenerIdCounter++;
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    private class Worker extends AbstractMonotonicWorker {
        private final Map<ITopic, AtomicLong> counterMap = new HashMap();
        private final String id = UUID.randomUUID().toString();
        final Random random = new Random();
        long messagesSend = 0;

        public Worker() {
            for (ITopic topic : topics) {
                counterMap.put(topic, new AtomicLong());
            }
        }

        @Override
        protected void timeStep() {
            ITopic topic = getRandomTopic();
            AtomicLong counter = counterMap.get(topic);
            MessageEntity msg = new MessageEntity(id, counter.incrementAndGet());
            messagesSend++;
            topic.publish(msg);
        }

        @Override
        protected void afterRun() {
            totalMessagesSend.addAndGet(messagesSend);
        }

        private ITopic getRandomTopic() {
            int index = random.nextInt(topics.length);
            return topics[index];
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    @Verify(global = true)
    public void verify() {

        final long expectedCount = listenersPerTopic * totalMessagesSend.get();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long actualCount = 0;
                for (MessageListenerImpl topicListener : listeners) {
                    actualCount += topicListener.received;
                }
                assertEquals("published messages don't match received messages", expectedCount, actualCount);
            }
        });

        assertEquals("Failures found", 0, failures.get());
    }

    public static class MessageDataSerializableFactory implements DataSerializableFactory {

        public static final int FACTORY_ID = 18;

        @Override
        public IdentifiedDataSerializable create(int i) {
            return new MessageEntity();
        }
    }

    private static class MessageEntity implements IdentifiedDataSerializable {
        private String thread;
        private long value;

        public MessageEntity() {
        }

        public MessageEntity(String thread, long counter) {
            this.thread = thread;
            this.value = counter;
        }

        @Override
        public String toString() {
            return "MessageEntity{" +
                    "thread=" + thread +
                    ", value=" + value +
                    '}';
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(thread);
            out.writeLong(value);

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            thread = in.readUTF();
            value = in.readLong();
        }

        @Override
        public int getFactoryId() {
            return MessageDataSerializableFactory.FACTORY_ID;
        }

        @Override
        public int getId() {
            return 0;
        }
    }

    private class MessageListenerImpl implements MessageListener<MessageEntity> {
        private final int id;
        private volatile long received = 0;
        private Map<String, Long> values = new HashMap<String, Long>();

        public MessageListenerImpl(int id) {
            this.id = id;
        }

        @Override
        public void onMessage(Message<MessageEntity> message) {
            String threadId = message.getMessageObject().thread;
            Long previousValue = values.get(threadId);
            if (previousValue == null) {
                previousValue = 0L;
            }

            long actualValue = message.getMessageObject().value;
            long expectedValue = previousValue + 1;
            if (expectedValue != actualValue) {
                failures.incrementAndGet();
                ExceptionReporter.report(testContext.getTestId(),
                        new RuntimeException("There is unexpected gap or equality between values, " +
                                "expected:" + expectedValue + " actual:" + actualValue));
            }

            values.put(threadId, actualValue);

            if (received % 100000 == 0) {
                LOGGER.info(toString() + " is at: " + message.getMessageObject().toString());
            }

            received++;
        }

        @Override
        public String toString() {
            return "StressMessageListener{" +
                    "id=" + id +
                    '}';
        }
    }

}
