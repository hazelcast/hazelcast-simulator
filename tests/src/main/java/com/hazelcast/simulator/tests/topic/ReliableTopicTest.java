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
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class ReliableTopicTest {

    private static final ILogger LOGGER = Logger.getLogger(ReliableTopicTest.class);

    // properties
    public String basename = ReliableTopicTest.class.getSimpleName();
    public int topicCount = 10;
    public int threadCount = 3;
    public int listenersPerTopic = 2;
    public KeyLocality keyLocality = KeyLocality.RANDOM;

    private AtomicLong failures = new AtomicLong();
    private IAtomicLong totalMessagesSend;
    private ITopic<MessageEntity>[] topics;
    private TestContext testContext;
    private List<MessageListenerImpl> listeners;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        totalMessagesSend = targetInstance.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        topics = new ITopic[topicCount];
        listeners = new LinkedList<MessageListenerImpl>();

        String[] names = generateStringKeys(basename + "-" + testContext.getTestId(), topicCount, keyLocality, targetInstance);

        int listenerIdCounter = 0;
        for (int i = 0; i < topics.length; i++) {
            ITopic<MessageEntity> topic = targetInstance.getReliableTopic(names[i]);
            topics[i] = topic;
            for (int l = 0; l < listenersPerTopic; l++) {
                MessageListenerImpl topicListener = new MessageListenerImpl(listenerIdCounter);
                listenerIdCounter++;
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    @Verify(global = true)
    public void verify() {
        final long expectedCount = listenersPerTopic * totalMessagesSend.get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long actualCount = 0;
                for (MessageListenerImpl topicListener : listeners) {
                    actualCount += topicListener.received.get();
                }
                assertEquals("published messages don't match received messages", expectedCount, actualCount);
            }
        });
        assertEquals("Failures found", 0, failures.get());
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private long messagesSend = 0;

        private final Map<ITopic, AtomicLong> counterMap = new HashMap<ITopic, AtomicLong>();
        private final String id = UUID.randomUUID().toString();

        public Worker() {
            for (ITopic topic : topics) {
                counterMap.put(topic, new AtomicLong());
            }
        }

        @Override
        protected void timeStep() throws Exception {
            ITopic<MessageEntity> topic = getRandomTopic();
            AtomicLong counter = counterMap.get(topic);
            MessageEntity msg = new MessageEntity(id, counter.incrementAndGet());
            messagesSend++;
            topic.publish(msg);
        }

        @Override
        protected void afterRun() {
            totalMessagesSend.addAndGet(messagesSend);
        }

        private ITopic<MessageEntity> getRandomTopic() {
            int index = randomInt(topics.length);
            return topics[index];
        }
    }

    private static class MessageDataSerializableFactory implements DataSerializableFactory {

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
            return "MessageEntity{"
                    + "thread=" + thread
                    + ", value=" + value
                    + '}';
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

        private final Map<String, Long> values = new HashMap<String, Long>();
        private final AtomicLong received = new AtomicLong();

        private final int id;

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
                ExceptionReporter.report(testContext.getTestId(), new TestException(format(
                        "There is an unexpected gap or equality between values. Expected %d, but was %d",
                        expectedValue, actualValue)));
            }

            values.put(threadId, actualValue);

            if (received.getAndIncrement() % 100000 == 0) {
                LOGGER.info(toString() + " is at " + message.getMessageObject().toString());
            }
        }

        @Override
        public String toString() {
            return "StressMessageListener{"
                    + "id=" + id
                    + '}';
        }
    }
}
