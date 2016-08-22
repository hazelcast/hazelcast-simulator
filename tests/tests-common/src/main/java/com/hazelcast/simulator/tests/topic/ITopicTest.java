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

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.hazelcast.simulator.utils.CommonUtils.sleepRandomNanos;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

/**
 * Creates a number of {@link ITopic} and a number of listeners per topic. Each member publishes messages to every topic.
 *
 * This test is inherently unreliable because the {@link ITopic} relies on the event system which is unreliable.
 * When messages are published with a too high rate, eventually the event system will ignore incoming events.
 */
public class ITopicTest extends AbstractTest {

    // properties
    public int topicCount = 1000;
    public int listenersPerTopic = 1;
    public int maxProcessingDelayNanos = 0;
    public int maxPublicationDelayNanos = 1000;
    // the maximum period the verification process is going to wait till the correct number of messags
    // have been received. A negative value indicates that no verification should be done.
    public int maxVerificationTimeSeconds = 60;

    private IAtomicLong totalExpectedCounter;
    private IAtomicLong totalFoundCounter;
    private ITopic[] topics;
    private List<TopicListener> listeners;

    @Setup
    public void setup() {
        totalExpectedCounter = targetInstance.getAtomicLong(name + ":TotalExpectedCounter");
        totalFoundCounter = targetInstance.getAtomicLong(name + ":TotalFoundCounter");

        topics = new ITopic[topicCount];
        listeners = new LinkedList<TopicListener>();
        for (int topicIndex = 0; topicIndex < topics.length; topicIndex++) {
            ITopic<Long> topic = targetInstance.getTopic(name + topicIndex);
            topics[topicIndex] = topic;

            for (int listenerIndex = 0; listenerIndex < listenersPerTopic; listenerIndex++) {
                TopicListener topicListener = new TopicListener();
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        sleepRandomNanos(state.random, maxPublicationDelayNanos);

        long msg = state.nextMessage();
        state.count += msg;

        ITopic<Long> topic = state.getRandomTopic();
        topic.publish(msg);
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalExpectedCounter.addAndGet(state.count);
    }

    public class ThreadState extends BaseThreadState {

        private long count;

        @SuppressWarnings("unchecked")
        private ITopic<Long> getRandomTopic() {
            int index = randomInt(topics.length);
            return (ITopic<Long>) topics[index];
        }

        private long nextMessage() {
            long msg = randomLong() % 1000;
            return (msg < 0) ? -msg : msg;
        }
    }

    private class TopicListener implements MessageListener<Long> {

        private final Random random = new Random();

        private volatile long count;

        @Override
        public void onMessage(Message<Long> message) {
            sleepRandomNanos(random, maxProcessingDelayNanos);
            count += message.getMessageObject();
        }
    }

    @Verify(global = true)
    public void verify() {
        if (maxVerificationTimeSeconds < 0) {
            return;
        }

        final long expectedCount = totalExpectedCounter.get();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                long actualCount = 0;
                for (TopicListener topicListener : listeners) {
                    actualCount += topicListener.count;
                }
                assertEquals("published messages don't match received messages", expectedCount, actualCount);
            }
        }, maxVerificationTimeSeconds);
    }

    @Teardown
    public void teardown() {
        for (ITopic topic : topics) {
            topic.destroy();
        }
        totalExpectedCounter.destroy();
        totalFoundCounter.destroy();
    }
}
