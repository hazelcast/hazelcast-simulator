package com;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestFailureException;
import com.hazelcast.stabilizer.tests.TestRunner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class ExampleTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(AtomicLongTest.class);

    private IAtomicLong totalCounter;
    private AtomicLong operations = new AtomicLong();

    //props
    public int threadCount = 1;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    private IAtomicLong counter;

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(getTestId() + ":TotalCounter");
        counter = targetInstance.getAtomicLong("counter");
    }

    @Override
    public void createTestThreads() {
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long foundCount = counter.get();

        if (expectedCount != foundCount) {
            throw new TestFailureException("Expected count: " + expectedCount + " but found count was: " + foundCount);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        counter.destroy();
        totalCounter.destroy();
    }

    @Override
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stopped()) {
                counter.incrementAndGet();

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            totalCounter.addAndGet(iteration);
        }

    }

    public static void main(String[] args) throws Exception {
        ExampleTest test = new ExampleTest();
        new TestRunner().run(test, 60);
    }
}

