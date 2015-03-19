/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.io.Serializable;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class ProducerConsumerTest {

    private static final ILogger log = Logger.getLogger(ProducerConsumerTest.class);

    //props
    public int producerCount = 4;
    public int consumerCount = 4;
    public int maxIntervalMillis = 1000;
    public String basename = "queue";

    private IAtomicLong produced;
    private IQueue workQueue;
    private IAtomicLong consumed;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        produced = targetInstance.getAtomicLong(basename + "-" + testContext.getTestId() + ":Produced");
        consumed = targetInstance.getAtomicLong(basename + "-" + testContext.getTestId() + ":Consumed");
        workQueue = targetInstance.getQueue(basename + "-" + testContext.getTestId() + ":WorkQueue");
    }

    @Teardown
    public void teardown() throws Exception {
        produced.destroy();
        workQueue.destroy();
        consumed.destroy();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < producerCount; k++) {
            spawner.spawn("ProducerThread", new Producer(k));
        }
        for (int k = 0; k < consumerCount; k++) {
            spawner.spawn("ConsumerThread", new Consumer(k));
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() {
        long expected = workQueue.size() + consumed.get();
        long actual = produced.get();
        assertEquals(expected, actual);
    }

    private class Producer implements Runnable {
        final Random rand = new Random(System.currentTimeMillis());
        final int id;

        public Producer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                try {
                    Thread.sleep(rand.nextInt(maxIntervalMillis) * consumerCount);
                    produced.incrementAndGet();
                    workQueue.offer(new Work());

                    iteration++;
                    if (iteration % 10 == 0) {
                        log.info(String.format(
                                "%s prod-id: %d, iteration: %d, produced: %d, workQueue: %d, consumed: %d",
                                Thread.currentThread().getName(), id, iteration,
                                produced.get(), workQueue.size(), consumed.get()
                        ));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private class Consumer implements Runnable {
        Random rand = new Random(System.currentTimeMillis());
        int id;

        public Consumer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                try {
                    workQueue.take();
                    consumed.incrementAndGet();
                    Thread.sleep(rand.nextInt(maxIntervalMillis) * producerCount);

                    iteration++;
                    if (iteration % 20 == 0) {
                        log.info(String.format(
                                "%s prod-id: %d, iteration: %d, produced: %d, workQueue: %d, consumed: %d",
                                Thread.currentThread().getName(), id, iteration,
                                produced.get(), workQueue.size(), consumed.get()
                        ));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class Work implements Serializable {
    }

    public static void main(String[] args) throws Throwable {
        ProducerConsumerTest test = new ProducerConsumerTest();
        new TestRunner<ProducerConsumerTest>(test).run();
    }
}

