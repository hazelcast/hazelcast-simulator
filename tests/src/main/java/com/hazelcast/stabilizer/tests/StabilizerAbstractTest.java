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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ExceptionReporter;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils;
import com.hazelcast.stabilizer.worker.OperationSelector;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public abstract class StabilizerAbstractTest {

    public static enum BaseOperation {
        PUT,
        PUT_ASYNC,
        GET,
        GET_ASYNC
    }

    private final static ILogger log = Logger.getLogger(StabilizerAbstractTest.class);

    // Properties
    public int threadCount = 10;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;

    // Member variables
    private TestContext testContext;
    private HazelcastInstance hazelcastInstance;
    private IAtomicLong verifyCounter;
    private AtomicLong performanceCounter = new AtomicLong();
    private OperationSelector<BaseOperation> selector = new OperationSelector<BaseOperation>();

    protected StabilizerAbstractTest addOperation(BaseOperation operation, double probability) {
        selector.addOperation(operation, probability);
        return this;
    }

    protected void addOperationRemainingProbability(BaseOperation operation) {
        selector.addOperationRemainingProbability(operation);
    }

    @Setup
    public void setup(TestContext context) throws Exception {
        testContext = context;
        hazelcastInstance = context.getTargetInstance();
        verifyCounter = hazelcastInstance.getAtomicLong("VerifyCounter:" + context.getTestId());

        afterSetup(hazelcastInstance);
    }

    @Teardown
    public void teardown() throws Exception {
        beforeTeardown();

        verifyCounter.destroy();
        log.info(HazelcastTestUtils.getOperationCountInformation(hazelcastInstance));
    }

    @Verify
    public void verify() throws Exception {
        doVerify(hazelcastInstance, verifyCounter.get());
    }

    @Performance
    public long getOperationCount() {
        return performanceCounter.get();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(createWorker());
        }
        spawner.awaitCompletion();
    }

    protected abstract void afterSetup(HazelcastInstance hazelcastInstance) throws Exception;

    protected abstract void beforeTeardown() throws Exception;

    protected abstract void doVerify(HazelcastInstance hazelcastInstance, long verifyCount) throws Exception;

    protected abstract BaseWorker createWorker();

    protected abstract class BaseWorker implements Runnable {
        private final Random random = new Random();

        long iterations = 0;
        long verifyCounterIncrements = 0;

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                doRun(selector.select());

                incrementIteration();
            }
            performanceCounter.addAndGet(iterations % performanceUpdateFrequency);
            verifyCounter.addAndGet(verifyCounterIncrements);
        }

        protected abstract void doRun(BaseOperation baseOperation);

        protected int getRandomInt(int upperBound) {
            return random.nextInt(upperBound);
        }

        protected void incrementVerifyCounter() {
            verifyCounterIncrements++;
        }

        void incrementIteration() {
            iterations++;
            if (iterations % logFrequency == 0) {
                log.info(Thread.currentThread().getName() + " At iteration: " + iterations);
            }
            if (iterations % performanceUpdateFrequency == 0) {
                performanceCounter.addAndGet(performanceUpdateFrequency);
            }
        }
    }

    protected abstract class AsyncBaseWorker<T> extends BaseWorker implements ExecutionCallback<T> {

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                doRun(selector.select());
            }
            performanceCounter.addAndGet(iterations % performanceUpdateFrequency);
            verifyCounter.addAndGet(verifyCounterIncrements);
        }

        @Override
        public void onResponse(T type) {
            incrementIteration();

            onAsyncResponse(type);
        }

        @Override
        public void onFailure(Throwable throwable) {
            reportException(throwable);
        }

        protected abstract void onAsyncResponse(T type);

        protected void reportException(Throwable throwable) {
            ExceptionReporter.report(testContext.getTestId(), throwable);
        }
    }
}
