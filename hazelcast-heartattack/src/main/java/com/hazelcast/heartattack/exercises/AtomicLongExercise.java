package com.hazelcast.heartattack.exercises;


import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Random;
import java.util.logging.Level;

public class AtomicLongExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(AtomicLongExercise.class);

    private int countersLength = 1000;
    private int threadCount = 1;

    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;

    @Override
    public void localSetup() {
        log.log(Level.INFO, "countersLength:" + countersLength + " threadCount:" + threadCount);

        totalCounter = hazelcastInstance.getAtomicLong(exerciseId + ":TotalCounter");
        counters = new IAtomicLong[countersLength];
        for (int k = 0; k < counters.length; k++) {
            counters[k] = hazelcastInstance.getAtomicLong(exerciseId + ":Counter-" + k);
        }

        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long count = 0;
        for (IAtomicLong counter : counters) {
            count += counter.get();
        }

        if (expectedCount != count) {
            throw new RuntimeException("Expected count: " + expectedCount + " but found count was: " + count);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stop) {
                int index = random.nextInt(counters.length);
                counters[index].incrementAndGet();
                if (iteration % 10000 == 0) {
                    log.log(Level.INFO, Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                iteration++;
            }

            totalCounter.addAndGet(iteration);
        }
    }
}

