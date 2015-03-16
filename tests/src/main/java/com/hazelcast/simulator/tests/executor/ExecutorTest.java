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
package com.hazelcast.simulator.tests.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.utils.TestUtils;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ExecutorTest {

    private static final ILogger log = Logger.getLogger(ExecutorTest.class);

    //props
    public int executorCount = 1;
    //the number of threads submitting tasks to the executor.
    public int threadCount = 5;
    //the number of outstanding submits, before doing get. A count of 1 means that you wait for every task
    //to complete, before sending in the next.
    public int submitCount = 5;
    public String basename = this.getClass().getSimpleName();

    private IExecutorService[] executors;
    private IAtomicLong executedCounter;
    private IAtomicLong expectedExecutedCounter;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        executors = new IExecutorService[executorCount];
        for (int k = 0; k < executors.length; k++) {
            executors[k] = targetInstance.getExecutorService(basename + "-" + testContext.getTestId() + "-" + k);
        }

        executedCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":ExecutedCounter");
        expectedExecutedCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":ExpectedExecutedCounter");
    }

    @Teardown(global = true)
    public void teardown() throws Exception {
        executedCounter.destroy();
        expectedExecutedCounter.destroy();
        for (IExecutorService executor : executors) {
            executor.shutdownNow();
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                log.severe("Time out while waiting for  shutdown of executor: " + executor.getId());
            }
            executor.destroy();
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() throws Exception {
        long actual = executedCounter.get();
        long expected = expectedExecutedCounter.get();
        assertEquals(expected, actual);
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

                for (int k = 0; k < submitCount; k++) {
                    Future future = executorService.submit(new Task(testContext.getTestId()));
                    futureList.add(future);
                    iteration++;
                }

                for (Future future : futureList) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (iteration % 10000 == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
            }
            expectedExecutedCounter.addAndGet(iteration);
        }
    }

    private static class Task implements Runnable, Serializable, HazelcastInstanceAware {

        private static final long serialVersionUID = 8301151618785236415L;

        private transient HazelcastInstance hz;
        private final String testId;

        private Task(String testId) {
            this.testId = testId;
        }

        @Override
        public void run() {
            ExecutorTest test = (ExecutorTest) hz.getUserContext().get(TestUtils.TEST_INSTANCE + ":" + testId);
            test.executedCounter.incrementAndGet();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }
    }

    public static void main(String[] args) throws Throwable {
        ExecutorTest test = new ExecutorTest();
        new TestRunner<ExecutorTest>(test).run();
    }
}
