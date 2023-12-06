/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.topic;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class ReliableTopicTest extends HazelcastTest {

    // properties
    public int topicCount = 10;
    public int listenersPerTopic = 2;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private AtomicLong failures = new AtomicLong();
    private IAtomicLong totalMessagesSend;
    private ITopic<MessageEntity>[] topics;
    private List<MessageListenerImpl> listeners;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        totalMessagesSend = getAtomicLong(name + ":TotalExpectedCounter");
        topics = new ITopic[topicCount];
        listeners = new LinkedList<>();

        String[] names = generateStringKeys(name, topicCount, keyLocality, targetInstance);

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
        private long messagesSend = 0;
        private final Map<ITopic<?>, AtomicLong> counterMap = new HashMap<>();
        private final String id = newSecureUuidString();

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

    private class MessageListenerImpl implements MessageListener<MessageEntity> {
        private final Map<String, Long> values = new HashMap<>();
        private final AtomicLong received = new AtomicLong();

        private final int id;

        public MessageListenerImpl(int id) {
            this.id = id;
        }

        @Override
        public void onMessage(Message<MessageEntity> message) {
            String threadId = message.getMessageObject().thread;
            long actualValue = message.getMessageObject().value;
            Long previousValue = values.put(threadId, actualValue);
            if (previousValue == null) {
                previousValue = 0L;
            }

            long expectedValue = previousValue + 1;
                                    
            if (expectedValue != actualValue) {
                failures.incrementAndGet();
                ExceptionReporter.report(testContext.getTestId(), new TestException(format(
                        "There is an unexpected gap or equality between values. Expected %d, but was %d",
                        expectedValue, actualValue)));
            }

            if (received.getAndIncrement() % 100000 == 0) {
                logger.info("{} is at {}", this, message.getMessageObject());
            }
        }

        @Override
        public String toString() {
            return "StressMessageListener{"
                    + "id=" + id
                    + '}';
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
