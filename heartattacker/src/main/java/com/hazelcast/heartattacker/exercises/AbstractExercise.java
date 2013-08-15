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
package com.hazelcast.heartattacker.exercises;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.heartattacker.Utils;
import com.hazelcast.heartattacker.performance.NotAvailable;
import com.hazelcast.heartattacker.performance.Performance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractExercise implements Exercise {

    private final static ILogger log = Logger.getLogger(AbstractExercise.class.getName());

    protected HazelcastInstance hazelcastInstance;

    protected String exerciseId;
    protected volatile boolean stop = false;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final Set<Thread> threads = new HashSet<Thread>();
    private long startMs;

    public String getExerciseId() {
        return exerciseId;
    }

    public void setExerciseId(String exerciseId) {
        this.exerciseId = exerciseId;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void globalSetup() throws Exception {
    }

    @Override
    public void localSetup() throws Exception {
    }

    @Override
    public void localTearDown() throws Exception {
    }

    @Override
    public void globalTearDown() throws Exception {
    }

    @Override
    public void globalVerify() throws Exception {
    }

    @Override
    public void localVerify() throws Exception {
    }

    public final Thread spawn(Runnable runnable) {
        Thread thread = new Thread(new CatchingRunnable(runnable));
        threads.add(thread);
        thread.start();
        return thread;
    }
    public long getCurrentTimeMs(){
        return hazelcastInstance.getCluster().getClusterTime();
    }

    public long getStartTimeMs(){
        return startMs;
    }

    @Override
    public Performance calcPerformance() {
        return new NotAvailable();
    }

    private class CatchingRunnable implements Runnable {
        private final Runnable runnable;

        private CatchingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                startLatch.await();
                runnable.run();
            } catch (Throwable t) {

                log.severe("Error detected", t);
                Utils.sleepSeconds(2);

                ExerciseUtils.signalHeartAttack(t);
            }
        }
    }

    @Override
    public void start() {
        startMs = hazelcastInstance.getCluster().getClusterTime();
        startLatch.countDown();
    }

    @Override
    public void stop(long timeout) throws InterruptedException {
        stop = true;

        for (Thread thread : threads) {
            //todo: we should calculate remaining timeout..
            thread.join(timeout);
        }
        threads.clear();
    }
}
