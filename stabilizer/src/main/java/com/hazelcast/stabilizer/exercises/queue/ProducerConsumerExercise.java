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
package com.hazelcast.stabilizer.exercises.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.exercises.AbstractExercise;
import com.hazelcast.stabilizer.exercises.ExerciseRunner;

import java.io.Serializable;
import java.util.Random;

public class ProducerConsumerExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(ProducerConsumerExercise.class);

    private IAtomicLong produced;
    private IQueue works;
    private IAtomicLong consumed;

    //properties
    public int producerCount = 4;
    public int consumerCount = 4;
    public int maxIntervalMillis = 1000;

    @Override
    public void localSetup() throws Exception {
        super.localSetup();

        log.info("producer:" + producerCount + " consumer:" + consumerCount);

        HazelcastInstance targetInstance = getTargetInstance();

        produced = targetInstance.getAtomicLong(exerciseId + ":Produced");
        consumed = targetInstance.getAtomicLong(exerciseId + ":Consumed");
        works = targetInstance.getQueue(exerciseId + ":WorkQueue");

        for (int k = 0; k < producerCount; k++) {
            spawn(new Producer(k));
        }
        for (int k = 0; k < consumerCount; k++) {
            spawn(new Consumer(k));
        }
    }

    @Override
    public void globalVerify() {
        long total = works.size() + consumed.get();
        long produced = this.produced.get();
        if (produced != total) {
            throw new RuntimeException("Produced count: " + produced + " but total: " + total);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        produced.destroy();
        works.destroy();
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
            while (!stop) {
                try {
                    Thread.sleep(rand.nextInt(maxIntervalMillis) * consumerCount);
                    produced.incrementAndGet();
                    works.offer(new Work());
                    iter++;
                    if (iter % 10 == 0) {
                        log.info(Thread.currentThread().getName() + " prod-id:" + id + " iteration: "
                                + iter + " prodoced:" + produced.get() + " workqueue:" + works.size() + " consumed:" + consumed.get());
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
            while (!stop) {
                try {
                    works.take();
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
                    + " produced:" + produced.get() + " workqueue:" + works.size() + " consumed:" + consumed.get());
        }
    }

    static class Work implements Serializable {
    }

    public static void main(String[] args) throws Exception {
        ProducerConsumerExercise exercise = new ProducerConsumerExercise();
        new ExerciseRunner().run(exercise, 180);
        System.exit(0);
    }
}

