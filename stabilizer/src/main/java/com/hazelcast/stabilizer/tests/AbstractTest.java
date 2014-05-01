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
package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.performance.NotAvailable;
import com.hazelcast.stabilizer.performance.Performance;
import com.hazelcast.stabilizer.worker.ExceptionReporter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractTest implements Test {

    private final static ILogger log = Logger.getLogger(AbstractTest.class);

    private HazelcastInstance serverInstance;
    private HazelcastInstance clientInstance;

    private String testId;
    private volatile boolean stop = false;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final Set<Thread> threads = new HashSet<Thread>();
    private long startMs;

    public HazelcastInstance getServerInstance() {
        return serverInstance;
    }

    public HazelcastInstance getClientInstance() {
        return clientInstance;
    }

    public String getTestId() {
        return testId;
    }

    public boolean stopped(){
        return stop;
    }

    @Override
    public void init(TestDependencies dependencies) {
        this.serverInstance = dependencies.serverInstance;
        this.clientInstance = dependencies.clientInstance;
        this.testId = dependencies.testId;
    }

    @Override
    public void globalSetup() throws Exception {
    }

    @Override
    public void localSetup() throws Exception {
    }

    public HazelcastInstance getTargetInstance() {
        if (clientInstance != null) {
            return clientInstance;
        } else {
            return serverInstance;
        }
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

    public long getCurrentTimeMs() {
        return getTargetInstance().getCluster().getClusterTime();
    }

    public long getStartTimeMs() {
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
                ExceptionReporter.report(t);
            }
        }
    }

    @Override
    public void start() {
        startMs = getCurrentTimeMs();
        startLatch.countDown();
    }

    @Override
    public void stop(long timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        long usedMs = 0;
        stop = true;

        for (Thread thread : threads) {
            if (timeout != 0) {
                usedMs = System.currentTimeMillis() - startTime;
                if (usedMs >= timeout) {
                    StringBuilder msg = new StringBuilder("Timeout while waiting for testing threads to stop.");
                    Thread.State threadState = thread.getState();
                    if (threadState != Thread.State.TERMINATED) {
                        msg.append(" Thread '")
                                .append(thread.getName())
                                .append("' is still in a state '")
                                .append(threadState)
                                .append("'.");
                    }
                    log.warning(msg.toString());
                    return; //todo: throw TimeoutException instead?
                }
            }
            thread.join(timeout - usedMs);

        }
        threads.clear();
    }
}
