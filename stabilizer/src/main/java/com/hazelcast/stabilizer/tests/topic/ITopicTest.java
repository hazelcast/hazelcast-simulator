package com.hazelcast.stabilizer.tests.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepRandom;
import static org.junit.Assert.assertEquals;


/**
 * Tests the ITopic. It test creates a number of topic and a number of listeners per topic and each member
 * publishes messages to every topic.
 * <p/>
 * This test is inherently unreliable because the ITopic relies on the event system which is unreliable. When messages
 * are published with a too high rate, eventually the eventsystem will drop incoming events.
 */
public class ITopicTest {

    //props
    public int topicCount = 1000;
    public int threadCount = 5;
    public int listenersPerTopic = 1;
    public int logFrequency = 100000;
    public int performanceUpdateFrequency = 100000;
    public int maxProcessingDelayNanos = 0;
    public int maxPublicationDelayNanos = 0;
    public boolean waitForMessagesToComplete = true;
    public String basename = "topic";

    private final static ILogger log = Logger.getLogger(ITopicTest.class);
    private IAtomicLong totalExpectedCounter;
    private IAtomicLong totalFoundCounter;
    private ITopic[] topics;
    private AtomicLong operations = new AtomicLong();
    private CountDownLatch listenersCompleteLatch;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        totalExpectedCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":TotalExpectedCounter");
        totalFoundCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":TotalFoundCounter");
        topics = new ITopic[topicCount];
        listenersCompleteLatch = new CountDownLatch(listenersPerTopic * topicCount);

        for (int k = 0; k < topics.length; k++) {
            ITopic<Long> topic = targetInstance.getTopic(basename + "-" + testContext.getTestId() + "-" + k);
            topics[k] = topic;

            for (int l = 0; l < listenersPerTopic; l++) {
                new TopicListener(topic);
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
        long expectedCount = totalExpectedCounter.get();
        long foundCount = totalFoundCounter.get();
        assertEquals(expectedCount, foundCount);
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


    private class TopicListener implements MessageListener<Long> {
        private final ITopic topic;
        private final String registrationId;
        private long count;
        private boolean completed = false;
        private final Random random = new Random();

        private TopicListener(ITopic topic) {
            this.topic = topic;
            registrationId = topic.addMessageListener(this);
        }

        @Override
        public void onMessage(Message<Long> message) {
            long payload = message.getMessageObject();

            sleepRandom(random, maxProcessingDelayNanos);

            if (isStopped(payload)) {
                totalFoundCounter.addAndGet(count);
                count = 0;

                if (!completed) {
                    completed = true;
                    listenersCompleteLatch.countDown();
                }
                topic.removeMessageListener(registrationId);
            } else {
                count += message.getMessageObject();
            }
        }

        private boolean isStopped(long payload) {
            return (!waitForMessagesToComplete && testContext.isStopped()) || payload < 0;
        }
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            long count = 0;
            while (!testContext.isStopped()) {
                sleepRandom(random, maxPublicationDelayNanos);

                ITopic topic = getRandomTopic();

                long msg = nextMessage();
                count += msg;
                topic.publish(msg);

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            for (ITopic topic : topics) {
                topic.publish(-1l);
            }

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

    public static void main(String[] args) throws Throwable {
        ITopicTest test = new ITopicTest();
        new TestRunner(test).run();
    }
}
