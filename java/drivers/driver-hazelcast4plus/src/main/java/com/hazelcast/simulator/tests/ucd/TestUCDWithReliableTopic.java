package com.hazelcast.simulator.tests.ucd;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static org.junit.Assert.assertEquals;

public class TestUCDWithReliableTopic extends HazelcastTest {
    // properties
    public int topicCount = 10;
    public int listenersPerTopic = 2;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private AtomicLong failures = new AtomicLong();
    protected IAtomicLong totalMessagesSend;
    protected ITopic<MessageEntity>[] topics;
    private List<MessageListenerImpl> listeners;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        totalMessagesSend = getAtomicLong(name + ":TotalExpectedCounter");
        topics = new ITopic[topicCount];

        String[] names = generateStringKeys(name, topicCount, keyLocality, targetInstance);

        for (int i = 0; i < topics.length; i++) {
            ITopic<MessageEntity> topic = targetInstance.getReliableTopic(names[i]);
            topics[i] = topic;
            for (int l = 0; l < listenersPerTopic; l++) {
                topic.addMessageListener(new MessageListenerImpl<>());
            }
        }
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        for (ITopic<?> topic : topics) {
            state.counterMap.put(topic, new AtomicLong());
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        ITopic<MessageEntity> topic = state.getRandomTopic();
        AtomicLong counter = state.counterMap.get(topic);
        MessageEntity msg = new MessageEntity(state.id, counter.incrementAndGet());
        state.messagesSend++;
        topic.publish(msg);
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalMessagesSend.addAndGet(state.messagesSend);
    }

    public class ThreadState extends BaseThreadState {
        public long messagesSend = 0;
        public final Map<ITopic<?>, AtomicLong> counterMap = new HashMap<>();
        public final String id = newSecureUuidString();

        public ITopic<MessageEntity> getRandomTopic() {
            int index = randomInt(topics.length);
            return topics[index];
        }
    }

    public static class MessageDataSerializableFactory implements DataSerializableFactory {
        public static final int FACTORY_ID = 18;

        @Override
        public IdentifiedDataSerializable create(int i) {
            return new MessageEntity();
        }
    }

    public static class MessageEntity implements IdentifiedDataSerializable {
        public String thread;
        public long value;

        public MessageEntity() {
        }

        public MessageEntity(String thread, long counter) {
            this.thread = thread;
            this.value = counter;
        }

        @Override
        public String toString() {
            return "MessageEntity{" + "thread=" + thread + ", value=" + value + '}';
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(thread);
            out.writeLong(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            thread = in.readString();
            value = in.readLong();
        }

        @Override
        public int getFactoryId() {
            return MessageDataSerializableFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    private class MessageListenerImpl<T> implements MessageListener<T> {
        private final AtomicLong received = new AtomicLong();

        @Override
        public void onMessage(Message<T> message) {
            received.incrementAndGet();
        }
    }

    @Verify(global = true)
    public void verify() {
        final long expectedCount = listenersPerTopic * totalMessagesSend.get();
        assertTrueEventually(() -> {
            long actualCount = listeners.stream().mapToLong(topicListener -> topicListener.received.get()).sum();
            assertEquals("published messages don't match received messages", expectedCount, actualCount);
        });
        assertEquals("Failures found", 0, failures.get());
    }
}
