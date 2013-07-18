package com.hazelcast.heartattack.exercises;

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Queue;
import java.util.logging.Level;

public class QueueExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(QueueExercise.class);

    private IAtomicLong totalCounter;
    private IQueue[] queues;
    private int queueLength = 100;
    private int threadsPerQueue = 1;
    private int messagesPerQueue = 1;

    @Override
    public void localSetup() {
        totalCounter = hazelcastInstance.getAtomicLong(getExerciseId() + ":TotalCounter");
        queues = new IQueue[queueLength];
        for (int k = 0; k < queues.length; k++) {
            queues[k] = hazelcastInstance.getQueue(exerciseId + ":Queue-" + k);
        }

        for (int queueIndex = 0; queueIndex < queueLength; queueIndex++) {
            for (int l = 0; l < threadsPerQueue; l++) {
                spawn(new Worker(queueIndex));
            }
        }

        for (IQueue<Long> queue : queues) {
            for (int k = 0; k < messagesPerQueue; k++) {
                queue.add(0L);
            }
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long count = 0;
        for (Queue<Long> queue : queues) {
            for (Long l : queue) {
                count += l;
            }
        }

        if (expectedCount != count) {
            throw new RuntimeException("Expected count: " + expectedCount + " but found count was: " + count);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        for (IQueue queue : queues) {
            queue.destroy();
        }
        totalCounter.destroy();
    }

    private class Worker implements Runnable {
        private final IQueue<Long> fromQueue;
        private final IQueue<Long> toQueue;

        public Worker(int fromIndex) {
            int toIndex = queueLength - 1 == fromIndex ? 0 : fromIndex + 1;
            this.fromQueue = queues[fromIndex];
            this.toQueue = queues[toIndex];
        }

        @Override
        public void run() {
            try {
                long iteration = 0;
                while (!stop) {
                    long item = fromQueue.take();
                    toQueue.put(item + 1);
                    if (iteration % 2000 == 0) {
                        log.log(Level.INFO, Thread.currentThread().getName() + " At iteration: " + iteration);
                    }
                    iteration++;
                }

                toQueue.put(0L);

                totalCounter.addAndGet(iteration);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        QueueExercise exercise = new QueueExercise();
        new ExerciseRunner().run(exercise, 60);
        System.exit(0);
    }
}

