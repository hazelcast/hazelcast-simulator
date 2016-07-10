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
package com.hazelcast.simulator.tests.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static org.junit.Assert.assertEquals;

public class ExecutorTest extends AbstractTest {

    // properties
    public int executorCount = 1;
    public int threadCount = 5;
    // the number of outstanding submits, before doing get. A count of 1 means that you wait for every task to complete,
    // before sending in the next
    public int submitCount = 5;

    private IExecutorService[] executors;
    private IAtomicLong executedCounter;
    private IAtomicLong expectedExecutedCounter;

    @Setup
    public void setup() {
        executors = new IExecutorService[executorCount];
        for (int i = 0; i < executors.length; i++) {
            executors[i] = targetInstance.getExecutorService(name + '-' + i);
        }

        executedCounter = targetInstance.getAtomicLong(name + ":ExecutedCounter");
        expectedExecutedCounter = targetInstance.getAtomicLong(name + ":ExpectedExecutedCounter");
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(name);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;

            List<Future> futureList = new LinkedList<Future>();
            while (!testContext.isStopped()) {
                int index = random.nextInt(executors.length);
                IExecutorService executorService = executors[index];
                futureList.clear();

                for (int i = 0; i < submitCount; i++) {
                    Future future = executorService.submit(new Task(testContext.getTestId()));
                    futureList.add(future);
                    iteration++;
                }

                for (Future future : futureList) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        throw rethrow(e);
                    }
                }

                if (iteration % 10000 == 0) {
                    logger.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
            }
            expectedExecutedCounter.addAndGet(iteration);
        }
    }

    private static final class Task implements Runnable, Serializable, HazelcastInstanceAware {

        private static final long serialVersionUID = 8301151618785236415L;

        private transient HazelcastInstance hz;
        private final String testId;

        private Task(String testId) {
            this.testId = testId;
        }

        @Override
        public void run() {
            ExecutorTest test = (ExecutorTest) hz.getUserContext().get(getUserContextKeyFromTestId(testId));
            test.executedCounter.incrementAndGet();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }
    }

    @Verify
    public void verify() {
        long actual = executedCounter.get();
        long expected = expectedExecutedCounter.get();
        assertEquals(expected, actual);
    }

    @Teardown(global = true)
    public void teardown() throws Exception {
        executedCounter.destroy();
        expectedExecutedCounter.destroy();
        for (IExecutorService executor : executors) {
            executor.shutdownNow();
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                logger.severe("Time out while waiting for shutdown of executor: " + executor.getName());
            }
            executor.destroy();
        }
    }

}
