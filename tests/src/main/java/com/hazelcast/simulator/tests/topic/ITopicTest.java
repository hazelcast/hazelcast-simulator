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
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.utils.AssertTask;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.sleepRandomNanos;
import static com.hazelcast.simulator.test.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

/**
 * Tests the ITopic. It test creates a number of topic and a number of listeners per topic and each member
 * publishes messages to every topic.
 *
 * This test is inherently unreliable because the ITopic relies on the event system which is unreliable.
 * When messages are published with a too high rate, eventually the event system will drop incoming events.
 */
public class ITopicTest {

    //props
    public int topicCount = 1000;
    public int threadCount = 5;
    public int listenersPerTopic = 1;
    public int logFrequency = 100000;
    public int performanceUpdateFrequency = 100000;
    public int maxProcessingDelayNanos = 0;
    public int maxPublicationDelayNanos = 1000;
    // the maximum period the verification process is going to wait till the correct number of messags
    // have been received. A negative value indicates that no verification should be done.
    public int maxVerificationTimeSeconds = 60;
    public String basename = "topic";

    private static final ILogger log = Logger.getLogger(ITopicTest.class);
    private IAtomicLong totalExpectedCounter;
    private IAtomicLong totalFoundCounter;
    private ITopic[] topics;
    private AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private HazelcastInstance hz;
    private List<TopicListener> listeners;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        hz = testContext.getTargetInstance();

        totalExpectedCounter = hz.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        totalFoundCounter = hz.getAtomicLong(testContext.getTestId() + ":TotalFoundCounter");
        topics = new ITopic[topicCount];
        listeners = new LinkedList<TopicListener>();
        for (int k = 0; k < topics.length; k++) {
            ITopic<Long> topic = hz.getTopic(basename + "-" + testContext.getTestId() + "-" + k);
            topics[k] = topic;

            for (int l = 0; l < listenersPerTopic; l++) {
                TopicListener topicListener = new TopicListener();
                topic.addMessageListener(topicListener);
                listeners.add(topicListener);
            }
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
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
    public void teardown() throws Exception {
        for (ITopic topic : topics) {
            topic.destroy();
        }
        totalExpectedCounter.destroy();
        totalFoundCounter.destroy();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            long count = 0;
            while (!testContext.isStopped()) {
                sleepRandomNanos(random, maxPublicationDelayNanos);

                ITopic topic = getRandomTopic();

                long msg = nextMessage();
                count += msg;
                topic.publish(msg);

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
            totalExpectedCounter.addAndGet(count);
        }

        private ITopic getRandomTopic() {
            int index = random.nextInt(topics.length);
            return topics[index];
        }

        private long nextMessage() {
            long msg = random.nextLong() % 1000;
            if (msg < 0) {
                msg = -msg;
            }
            return msg;
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

    public static void main(String[] args) throws Throwable {
        ITopicTest test = new ITopicTest();
        new TestRunner<ITopicTest>(test).withDuration(10).run();
    }
}
