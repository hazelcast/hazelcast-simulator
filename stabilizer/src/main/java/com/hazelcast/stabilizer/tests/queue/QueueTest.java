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
import com.hazelcast.stabilizer.tests.TestRunner;

import java.util.Queue;

public class QueueTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(QueueTest.class);

    private IAtomicLong totalCounter;
    private IQueue[] queues;

    //properties
    public int queueLength = 100;
    public int threadsPerQueue = 1;
    public int messagesPerQueue = 1;
    public String basename = "queue";

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(getTestId() + ":TotalCounter");
        queues = new IQueue[queueLength];
        for (int k = 0; k < queues.length; k++) {
            queues[k] = targetInstance.getQueue(basename + "-" + getTestId() + "-" + k);
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
                while (!stopped()) {
                    long item = fromQueue.take();
                    toQueue.put(item + 1);
                    if (iteration % 2000 == 0) {
                        log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
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
        QueueTest test = new QueueTest();
        new TestRunner().run(test, 60);
        System.exit(0);
    }
}

