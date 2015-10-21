package com.hazelcast.simulator.tests.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

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
 * When messages are published with a too high rate, eventually the event system will drop incoming events.
 */
public class ITopicTest {

    // properties
    public String basename = ITopicTest.class.getSimpleName();
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
    public void setup(TestContext testContext) throws Exception {
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        totalExpectedCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        totalFoundCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":TotalFoundCounter");

        topics = new ITopic[topicCount];
        listeners = new LinkedList<TopicListener>();
        for (int topicIndex = 0; topicIndex < topics.length; topicIndex++) {
            ITopic<Long> topic = targetInstance.getTopic(basename + '-' + testContext.getTestId() + '-' + topicIndex);
            topics[topicIndex] = topic;

            for (int listenerIndex = 0; listenerIndex < listenersPerTopic; listenerIndex++) {
                TopicListener topicListener = new TopicListener();
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (ITopic topic : topics) {
            topic.destroy();
        }
        totalExpectedCounter.destroy();
        totalFoundCounter.destroy();
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

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private long count;

        @Override
        public void timeStep() {
            sleepRandomNanos(getRandom(), maxPublicationDelayNanos);

            long msg = nextMessage();
            count += msg;

            ITopic<Long> topic = getRandomTopic();
            topic.publish(msg);
        }

        @Override
        protected void afterRun() {
            totalExpectedCounter.addAndGet(count);
        }

        @SuppressWarnings("unchecked")
        private ITopic<Long> getRandomTopic() {
            int index = randomInt(topics.length);
            return (ITopic<Long>) topics[index];
        }

        private long nextMessage() {
            long msg = getRandom().nextLong() % 1000;
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

    public static void main(String[] args) throws Exception {
        ITopicTest test = new ITopicTest();
        new TestRunner<ITopicTest>(test).withDuration(10).run();
    }
}
