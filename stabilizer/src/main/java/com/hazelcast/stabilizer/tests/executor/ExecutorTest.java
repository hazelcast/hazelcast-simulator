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
package com.hazelcast.stabilizer.tests.executor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestFailureException;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.annotations.Verify;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecutorTest {

    private final static ILogger log = Logger.getLogger(ExecutorTest.class);

    //props
    public int executorCount = 1;
    //the number of threads submitting tasks to the executor.
    public int threadCount = 5;
    //the number of outstanding submits, before doing get. A count of 1 means that you wait for every task
    //to complete, before sending in the next.
    public int submitCount = 5;
    public String basename = "executor";

    private IExecutorService[] executors;
    private IAtomicLong executedCounter;
    private IAtomicLong expectedExecutedCounter;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        executors = new IExecutorService[executorCount];
        for (int k = 0; k < executors.length; k++) {
            executors[k] = targetInstance.getExecutorService(basename + "-" + testContext.getTestId() + "-" + k);
        }

        executedCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":ExecutedCounter");
        expectedExecutedCounter = targetInstance.getAtomicLong(testContext.getTestId() + ":ExpectedExecutedCounter");
    }

    @Teardown
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
        ThreadSpawner spawner = new ThreadSpawner();

        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

     @Verify
    public void verify() throws Exception {
        log.info("globalVerify called");
        long actualCount = executedCounter.get();
        long expectedCount = expectedExecutedCounter.get();
        if (actualCount != expectedCount) {
            throw new TestFailureException("ActualCount:" + actualCount + " doesn't match ExpectedCount:" + expectedCount);
        }
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
                    Future future = executorService.submit(new Task());
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
        private transient HazelcastInstance hz;

        @Override
        public void run() {
            ExecutorTest test = (ExecutorTest) hz.getUserContext().get(TestUtils.TEST_INSTANCE);
            test.executedCounter.incrementAndGet();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }
    }
}
