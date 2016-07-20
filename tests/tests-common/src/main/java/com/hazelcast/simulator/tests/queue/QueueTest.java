/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IQueue;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static com.hazelcast.simulator.tests.helpers.KeyLocality.RANDOM;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

public class QueueTest extends AbstractTest {

    // properties
    public int queueLength = 100;
    public int threadsPerQueue = 1;
    public int messagesPerQueue = 1;

    private final AtomicLong totalCounter = new AtomicLong(0);
    private IQueue<Long>[] queues;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        queues = new IQueue[queueLength];

        // the KeyLocality has to be RANDOM here, since we need different queues on each Worker
        String[] names = generateStringKeys(name, queueLength, name.length() + 5, RANDOM, targetInstance);
        for (int i = 0; i < queues.length; i++) {
            queues[i] = targetInstance.getQueue(names[i]);
        }

        for (IQueue<Long> queue : queues) {
            for (int i = 0; i < messagesPerQueue; i++) {
                queue.add(0L);
            }
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(name);
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
        }

        @Override
        public void run() {
            try {
                long iteration = 0;

                while (!testContext.isStopped()) {
                    long item = fromQueue.take();
                    toQueue.put(item + 1);

                    iteration++;
                }

                toQueue.put(0L);
                totalCounter.addAndGet(iteration);
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
        }
    }

    @Verify
    public void verify() {
        long expected = totalCounter.get();
        long actual = 0;
        for (Queue<Long> queue : queues) {
            for (Long queueCounter : queue) {
                actual += queueCounter;
            }
        }

        assertEquals(expected, actual);
    }

    @Teardown
    public void teardown() {
        for (IQueue queue : queues) {
            queue.destroy();
        }
    }
}
