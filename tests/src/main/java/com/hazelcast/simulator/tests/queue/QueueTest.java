/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

public class QueueTest {

    private static final ILogger LOGGER = Logger.getLogger(QueueTest.class);

    // properties
    public String basename = QueueTest.class.getSimpleName();
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int queueLength = 100;
    public int threadsPerQueue = 1;
    public int messagesPerQueue = 1;
    public int logFrequency = 200;

    private final AtomicLong totalCounter = new AtomicLong(0);

    private TestContext testContext;
    private IQueue<Long>[] queues;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        queues = new IQueue[queueLength];

        String prefix = basename + '-' + testContext.getTestId() + '-';
        String[] names = generateStringKeys(prefix, queueLength, prefix.length() + 5, keyLocality, targetInstance);
        for (int i = 0; i < queues.length; i++) {
            queues[i] = targetInstance.getQueue(names[i]);
        }

        for (IQueue<Long> queue : queues) {
            for (int i = 0; i < messagesPerQueue; i++) {
                queue.add(0L);
            }
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (IQueue queue : queues) {
            queue.destroy();
        }
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

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int queueIndex = 0; queueIndex < queueLength; queueIndex++) {
            for (int i = 0; i < threadsPerQueue; i++) {
                spawner.spawn(new Worker(queueIndex));
            }
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {

        private final IQueue<Long> fromQueue;
        private final IQueue<Long> toQueue;

        public Worker(int fromIndex) {
            int toIndex = (queueLength - 1 == fromIndex) ? 0 : fromIndex + 1;
            fromQueue = queues[fromIndex];
            toQueue = queues[toIndex];

            LOGGER.info(String.format(
                    "%s fromQueue[%d] %s: %d, toQueue[%d] %s: %d",
                    Thread.currentThread().getName(),
                    fromIndex, fromQueue.getName(), fromQueue.size(),
                    toIndex, toQueue.getName(), toQueue.size()
            ));
        }

        @Override
        public void run() {
            try {
                long iteration = 0;

                while (!testContext.isStopped()) {
                    long item = fromQueue.take();
                    toQueue.put(item + 1);

                    iteration++;
                    if (logFrequency > 0 && iteration % logFrequency == 0) {
                        LOGGER.info(String.format(
                                "%s iteration: %d, fromQueue size: %d, toQueue size: %d",
                                Thread.currentThread().getName(), iteration, fromQueue.size(), toQueue.size()
                        ));
                    }
                }

                toQueue.put(0L);
                totalCounter.addAndGet(iteration);
            } catch (InterruptedException e) {
                throw new TestException(e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        QueueTest test = new QueueTest();
        new TestRunner<QueueTest>(test).run();
    }
}
