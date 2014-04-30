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
package com.hazelcast.stabilizer.tests.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestFailureException;
import com.hazelcast.stabilizer.tests.TestRunner;

import java.io.Serializable;
import java.util.Random;

public class ProducerConsumerTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(ProducerConsumerTest.class);

    private IAtomicLong produced;
    private IQueue workQueue;
    private IAtomicLong consumed;

    //properties
    public int producerCount = 4;
    public int consumerCount = 4;
    public int maxIntervalMillis = 1000;
    public String basename = "queue";

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        produced = targetInstance.getAtomicLong(basename + "-" + getTestId() + ":Produced");
        consumed = targetInstance.getAtomicLong(basename + "-" + getTestId() + ":Consumed");
        workQueue = targetInstance.getQueue(basename + "-" + getTestId() + ":WorkQueue");

        for (int k = 0; k < producerCount; k++) {
            spawn(new Producer(k));
        }
        for (int k = 0; k < consumerCount; k++) {
            spawn(new Consumer(k));
        }
    }

    @Override
    public void globalVerify() {
        long total = workQueue.size() + consumed.get();
        long produced = this.produced.get();
        if (produced != total) {
            throw new TestFailureException("Produced count: " + produced + " but total: " + total);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        produced.destroy();
        workQueue.destroy();
        consumed.destroy();
    }

    private class Producer implements Runnable {
        Random rand = new Random(System.currentTimeMillis());

        int id;

        public Producer(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            long iter = 0;
            while (!stopped()) {
                try {
                    Thread.sleep(rand.nextInt(maxIntervalMillis) * consumerCount);
                    produced.incrementAndGet();
                    workQueue.offer(new Work());
                    iter++;
                    if (iter % 10 == 0) {
                        log.info(Thread.currentThread().getName() + " prod-id:" + id + " iteration: "
                                + iter + " prodoced:" + produced.get() + " workqueue:" + workQueue.size() + " consumed:" + consumed.get());
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
            long iter = 0;
            while (!stopped()) {
                try {
                    workQueue.take();
                    consumed.incrementAndGet();
                    Thread.sleep(rand.nextInt(maxIntervalMillis) * producerCount);
                    iter++;
                    if (iter % 20 == 0) {
                        logState(iter);
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void logState(long iter) {
            log.info(Thread.currentThread().getName() + " prod-id:" + id + " iteration: " + iter
                    + " produced:" + produced.get() + " workqueue:" + workQueue.size() + " consumed:" + consumed.get());
        }
    }

    static class Work implements Serializable {
    }

    public static void main(String[] args) throws Exception {
        ProducerConsumerTest test = new ProducerConsumerTest();
        new TestRunner().run(test, 180);
        System.exit(0);
    }
}

