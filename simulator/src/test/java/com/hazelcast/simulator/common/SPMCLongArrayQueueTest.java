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

package com.hazelcast.simulator.common;

import com.hazelcast.util.collection.LongHashSet;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SPMCLongArrayQueueTest {

    static final int CAPACITY = 128;
    static final int ENQUEUE_COUNT = 10 * 1000 * 1000;
    static final int CONSUMER_COUNT = Runtime.getRuntime().availableProcessors() - 1;
    static final int CONSUMER_CAPACITY_HEADROOM = 10000;
    private static final long NULL_SENTINEL = 0;

    private final SPMCLongArrayQueue queue = new SPMCLongArrayQueue(CAPACITY, NULL_SENTINEL);

    @Test
    public void stressTest() throws InterruptedException {
        final Thread[] consumers = new Thread[CONSUMER_COUNT];
        final Worker[] workers = new Worker[CONSUMER_COUNT];
        for (int i = 0; i < consumers.length; i++) {
            workers[i] = new Worker();
            consumers[i] = new Thread(workers[i]);
            consumers[i].start();
        }
        System.out.format("Producing %,d items%n", ENQUEUE_COUNT);
        final long start = System.nanoTime();
        for (long val = 1; val <= ENQUEUE_COUNT; val++) {
            while (!queue.offer(val));
        }
        final double elapsedSeconds = (System.nanoTime() - start) / (double) SECONDS.toNanos(1);
        System.out.format("Production done, %,.0f items/sec%n", ENQUEUE_COUNT / elapsedSeconds);
        Thread.sleep(100);
        assertEquals("dequeued count mismatches enqueued count", queue.addedCount(), queue.removedCount());
        for (Thread consumer : consumers) {
            consumer.interrupt();
            consumer.join();
        }
        System.out.println("Consumers killed, validating");
        final LongHashSet allDequeued = new LongHashSet(ENQUEUE_COUNT, NULL_SENTINEL);
        int allDequeuedCount = 0;
        for (Worker w : workers) {
            for (int i = 0; i < w.insertionPoint; i++) {
                assertTrue("Duplicate dequeued value", allDequeued.add(w.dequeued[i]));
                if (++allDequeuedCount > ENQUEUE_COUNT) {
                    fail("Dequeued count exceeds enqueued count");
                }
            }
        }
        assertEquals("wrong allDequeued size", ENQUEUE_COUNT, allDequeued.size());
        for (long val = 1; val <= ENQUEUE_COUNT; val++) {
            if (!allDequeued.contains(val)) {
                fail("Failed to dequeue " + val);
            }
        }
    }

    class Worker implements Runnable {
        private final long[] dequeued = new long[ENQUEUE_COUNT / CONSUMER_COUNT + CONSUMER_CAPACITY_HEADROOM];
        int insertionPoint;

        @Override
        public void run() {
            while (!currentThread().isInterrupted() && insertionPoint < dequeued.length) {
                final long got = queue.poll();
                if (got != NULL_SENTINEL) {
                    dequeued[insertionPoint++] = got;
                }
            }
            System.out.format("Worker dequeued %,d items more than its fair share%n",
                    insertionPoint - ENQUEUE_COUNT / CONSUMER_COUNT);
        }
    }
}
