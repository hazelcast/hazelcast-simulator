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

import java.util.Queue;

import static org.junit.Assert.assertEquals;

public class QueueTest {

    private static final ILogger log = Logger.getLogger(QueueTest.class);

    private IAtomicLong totalCounter;
    private IQueue[] queues;

    //props
    public int queueLength = 100;
    public int threadsPerQueue = 1;
    public int messagesPerQueue = 1;
    public String basename = this.getClass().getSimpleName();
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":TotalCounter");
        queues = new IQueue[queueLength];
        for (int k = 0; k < queues.length; k++) {
            queues[k] = targetInstance.getQueue(basename + "-" + testContext.getTestId() + "-" + k);
        }

        for (IQueue<Long> queue : queues) {
            for (int k = 0; k < messagesPerQueue; k++) {
                queue.add(0L);
            }
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (IQueue queue : queues) {
            queue.destroy();
        }
        totalCounter.destroy();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int queueIndex = 0; queueIndex < queueLength; queueIndex++) {
            for (int l = 0; l < threadsPerQueue; l++) {
                spawner.spawn(new Worker(queueIndex));
            }
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() {
        long expected = totalCounter.get();
        long actual = 0;
        for (Queue<Long> queue : queues) {
            for (Long l : queue) {
                actual += l;
            }
        }

        assertEquals(expected, actual);
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
                while (!testContext.isStopped()) {
                    long item = fromQueue.take();
                    toQueue.put(item + 1);

                    iteration++;
                    if (iteration % 200 == 0) {
                        log.info(String.format(
                                "%s iteration: %d, fromQueue size: %d, toQueue size: %d",
                                Thread.currentThread().getName(), iteration, fromQueue.size(), toQueue.size()
                        ));
                    }
                }

                toQueue.put(0L);
                totalCounter.addAndGet(iteration);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        QueueTest test = new QueueTest();
        new TestRunner<QueueTest>(test).run();
    }
}