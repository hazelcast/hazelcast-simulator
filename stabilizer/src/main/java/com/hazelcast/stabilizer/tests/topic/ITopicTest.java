package com.hazelcast.stabilizer.tests.topic;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.performance.OperationsPerSecond;
import com.hazelcast.stabilizer.performance.Performance;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ITopicTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(ITopicTest.class);

    public int topicCount = 1000;
    public int threadCount = 5;
    public int listenersPerTopic = 1;
    public int logFrequency = 100000;
    public int performanceUpdateFrequency = 100000;
    public int processingDelayMillis = 0;
    public boolean waitForMessagesToComplete = true;

    private IAtomicLong totalExpectedCounter;
    private IAtomicLong totalFoundCounter;
    private ITopic[] topics;
    private AtomicLong operations = new AtomicLong();
    private CountDownLatch listenersCompleteLatch;

    @Override
    public void localSetup() throws Exception {
        super.localSetup();

        HazelcastInstance targetInstance = getTargetInstance();

        totalExpectedCounter = targetInstance.getAtomicLong(testId + ":TotalExpectedCounter");
        totalFoundCounter = targetInstance.getAtomicLong(testId + ":TotalFoundCounter");
        topics = new ITopic[topicCount];
        listenersCompleteLatch = new CountDownLatch(listenersPerTopic * topicCount);

        for (int k = 0; k < topics.length; k++) {
            ITopic<Long> topic = targetInstance.getTopic(testId + ":Topic-" + k);
            topics[k] = topic;

            for (int l = 0; l < listenersPerTopic; l++) {
                new TopicListener(topic);
            }
        }

        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalExpectedCounter.get();
        long foundCount = totalFoundCounter.get();

        if (expectedCount != foundCount) {
            throw new RuntimeException("Expected count: " + expectedCount + " but found count was: " + foundCount);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        for (ITopic topic : topics) {
            topic.destroy();
        }
        totalExpectedCounter.destroy();
        totalFoundCounter.destroy();
    }

    @Override
    public Performance calcPerformance() {
        OperationsPerSecond performance = new OperationsPerSecond();
        performance.setStartMs(getStartTimeMs());
        performance.setEndMs(getCurrentTimeMs());
        performance.setOperations(operations.get());
        return performance;
    }

    @Override
    public void stop(long timeoutMs) throws InterruptedException {
        //todo: we should calculate remining timeout
        super.stop(timeoutMs);

        boolean completed = listenersCompleteLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (!completed) {
            throw new RuntimeException("Timeout while waiting TopicListeners to complete");
        }
    }

    private class TopicListener implements MessageListener<Long> {
        private final ITopic topic;
        private final String registrationId;
        private long count;
        private boolean completed = false;

        private TopicListener(ITopic topic) {
            this.topic = topic;
            registrationId = topic.addMessageListener(this);
        }

        @Override
        public void onMessage(Message<Long> message) {
            long l = message.getMessageObject();

            if (processingDelayMillis > 0) {
                try {
                    Thread.sleep(processingDelayMillis);
                } catch (InterruptedException e) {
                }
            }

            boolean stopped = (!waitForMessagesToComplete && stop) || l < 0;

            if (stopped) {
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
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            long count = 0;
            while (!stop) {
                int index = random.nextInt(topics.length);
                ITopic topic = topics[index];

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

        private long nextMessage() {
            long msg = random.nextLong() % 1000;
            if (msg < 0) {
                msg = -msg;
            }
            return msg;
        }
    }

    public static void main(String[] args) throws Exception {
        ITopicTest test = new ITopicTest();
        new TestRunner().run(test, 10);
    }
}
