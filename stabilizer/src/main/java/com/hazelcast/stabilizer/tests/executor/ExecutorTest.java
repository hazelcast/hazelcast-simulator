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
import com.hazelcast.stabilizer.tests.AbstractTest;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecutorTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(ExecutorTest.class);

    private IExecutorService[] executors;
    private IAtomicLong executedCounter;
    private IAtomicLong expectedExecutedCounter;

    public int executorCount = 1;

    //the number of threads submitting tasks to the executor.
    public int threadCount = 5;

    //the number of outstanding submits, before doing get. A count of 1 means that you wait for every task
    //to complete, before sending in the next.
    public int submitCount = 5;

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        executors = new IExecutorService[executorCount];
        for (int k = 0; k < executors.length; k++) {
            executors[k] = targetInstance.getExecutorService(getTestId() + ":Executor-" + k);
        }
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
        executedCounter = targetInstance.getAtomicLong(getTestId() + ":ExecutedCounter");
        expectedExecutedCounter = targetInstance.getAtomicLong(getTestId() + ":ExpectedExecutedCounter");
    }

    @Override
    public void globalTearDown() throws Exception {
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

    @Override
    public void globalVerify() throws Exception {
        log.info("globalVerify called");
        long actualCount = executedCounter.get();
        long expectedCount = expectedExecutedCounter.get();
        if (actualCount != expectedCount) {
            throw new RuntimeException("ActualCount:" + actualCount + " doesn't match ExpectedCount:" + expectedCount);
        }
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;

            List<Future> futureList = new LinkedList<Future>();
            while (!stop()) {
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
            ExecutorTest test = (ExecutorTest) hz.getUserContext().get(TEST_INSTANCE);
            test.executedCounter.incrementAndGet();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }
    }
}
