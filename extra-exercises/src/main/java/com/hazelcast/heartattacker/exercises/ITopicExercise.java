package com.hazelcast.heartattacker.exercises;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.heartattacker.performance.OperationsPerSecond;
import com.hazelcast.heartattacker.performance.Performance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ITopicExercise extends AbstractExercise{

    private final static ILogger log = Logger.getLogger(ITopicExercise.class);

    public int topicCount = 1000;
    public int threadCount = 5;
    public int listenersPerTopic = 1;
    public int logFrequency = 100000;
    public int performanceUpdateFrequency = 100000;
    public int processingDelayMillis = 0;

    private IAtomicLong totalExpectedCounter;
    private IAtomicLong totalFoundCounter;
    private ITopic[] topics;
    private AtomicLong operations = new AtomicLong();
    private CountDownLatch listenersCompleteLatch;

    @Override
    public void localSetup() {
        totalExpectedCounter = hazelcastInstance.getAtomicLong(exerciseId + ":TotalExpectedCounter");
        totalFoundCounter = hazelcastInstance.getAtomicLong(exerciseId+":TotalFoundCounter");
        topics = new ITopic[topicCount];
        listenersCompleteLatch = new CountDownLatch(listenersPerTopic*topicCount);

        for (int k = 0; k < topics.length; k++) {
            ITopic<Long> topic = hazelcastInstance.getTopic(exerciseId + ":Topic-" + k);
            topics[k] = topic;

            for(int l=0;l<listenersPerTopic;l++){
                topic.addMessageListener(new TopicListener());
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
    public void stop() throws InterruptedException {
        super.stop();
        boolean completed = listenersCompleteLatch.await(60000, TimeUnit.SECONDS);
        if(!completed){
            throw new RuntimeException("Timeout while waiting TopicListeners to complete");
        }
    }

    private class TopicListener implements MessageListener<Long>{
        private long count;
        private boolean completed = false;

        @Override
        public void onMessage(Message<Long> message) {


            long l = message.getMessageObject();

            if(processingDelayMillis>0){
                try {
                    Thread.sleep(processingDelayMillis);
                } catch (InterruptedException e) {
                }
            }

            if(l<0){
                totalFoundCounter.addAndGet(count);
                count = 0;

                if(!completed){
                    completed = true;
                    listenersCompleteLatch.countDown();
                }
            }else{
                count+=message.getMessageObject();
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
                count+=msg;

                topic.publish(msg);

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if(iteration % performanceUpdateFrequency == 0){
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            for(ITopic topic: topics){
                topic.publish(-1l);
            }

            totalExpectedCounter.addAndGet(count);
        }

        private long nextMessage() {
            long msg = random.nextLong()%1000;
            if(msg <0){
                msg = -msg;
            }
            return msg;
        }
    }

    public static void main(String[] args) throws Exception {
        ITopicExercise mapExercise = new ITopicExercise();
        new ExerciseRunner().run(mapExercise, 10);
    }
}
