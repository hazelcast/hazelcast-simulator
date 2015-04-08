package com.hazelcast.simulator.tests.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
 * Tests the {@link ITopic}. It creates a number of topics and a number of listeners per topic and each member publishes messages
 * to every topic.
 *
 * With the default properties this test is inherently unreliable because the {@link ITopic} relies on the event system which is
 * unreliable. When messages are published with a too high rate, eventually the event system will drop incoming events.
 *
 * You can configure this test with reliable configuration (backed by a ringbuffer) by changing the basename to match the wildcard
 * <tt>ReliableITopic*</tt>.
 */
public class ITopicTest {

    private static final ILogger LOGGER = Logger.getLogger(ITopicTest.class);

    // properties
    public String basename = ITopicTest.class.getSimpleName();
    public int topicCount = 1000;
    public int threadCount = 5;
    public int listenersPerTopic = 1;
    public int logFrequency = 100000;
    public int performanceUpdateFrequency = 100000;
    public int maxProcessingDelayNanos = 0;
    public int maxPublicationDelayNanos = 1000;
    // the maximum period the verification process is going to wait till the correct number of messages have been received
    // a negative value indicates that no verification should be done
    public int maxVerificationTimeSeconds = 60;

    @SuppressWarnings("unchecked")
    private final ITopic<Long>[] topics = new ITopic[topicCount];
    private final List<TopicListener> listeners = new LinkedList<TopicListener>();

    private IAtomicLong totalExpectedCounter;
    private IAtomicLong totalFoundCounter;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        checkForRingbufferFeature();

        HazelcastInstance hz = testContext.getTargetInstance();
        String testId = testContext.getTestId();

        totalExpectedCounter = hz.getAtomicLong(testId + ":TotalExpectedCounter");
        totalFoundCounter = hz.getAtomicLong(testId + ":TotalFoundCounter");

        String topicBaseName = basename + "-" + testId + "-";
        for (int i = 0; i < topics.length; i++) {
            ITopic<Long> topic = hz.getTopic(topicBaseName + i);
            topics[i] = topic;

            for (int j = 0; j < listenersPerTopic; j++) {
                TopicListener topicListener = new TopicListener();
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    private void checkForRingbufferFeature() {
        Class classType = null;
        try {
            classType = Class.forName("com.hazelcast.config.RingbufferConfig");
        } catch (ClassNotFoundException e) {
            LOGGER.warning("Feature is not enabled in this version of Hazelcast!", e);
        }

        try {
            if (classType != null) {
                int capacity = (Integer) classType.getDeclaredField("DEFAULT_CAPACITY").get(null);
                LOGGER.info(basename + ": Ringbuffer capacity: " + capacity);
            }
        } catch (NoSuchFieldException e) {
            LOGGER.severe("Could not find expected field!", e);
        } catch (IllegalAccessException e) {
            LOGGER.severe("Could not read expected field!", e);
        }
    }

    @Teardown
    public void tearDown() throws Exception {
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
                    actualCount += topicListener.receivedMessages;
                }
                assertEquals("Number of published messages don't match received messages", expectedCount, actualCount);
            }
        }, maxVerificationTimeSeconds);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private long publishedMessages;

        @Override
        protected void timeStep() {
            sleepRandomNanos(getRandom(), maxPublicationDelayNanos);

            long msg = nextMessage();
            getRandomTopic().publish(msg);

            publishedMessages += msg;
        }

        @Override
        protected void afterRun() {
            totalExpectedCounter.addAndGet(publishedMessages);
        }

        private ITopic<Long> getRandomTopic() {
            int index = randomInt(topics.length);
            return topics[index];
        }

        private long nextMessage() {
            long msg = getRandom().nextLong() % 1000;
            if (msg < 0) {
                msg = -msg;
            }
            return msg;
        }
    }

    private class TopicListener implements MessageListener<Long> {

        private final Random random = new Random();

        private long receivedMessages;

        @Override
        public void onMessage(Message<Long> message) {
            sleepRandomNanos(random, maxProcessingDelayNanos);
            receivedMessages += message.getMessageObject();
        }
    }

    public static void main(String[] args) throws Exception {
        ITopicTest test = new ITopicTest();
        new TestRunner<ITopicTest>(test).withDuration(10).run();
    }
}
