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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.AfterWarmup;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.common.TestPhase.WARMUP;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TimeStepRunStrategyIntegrationTest {

    private static final String TEST_ID = "SomeId";

    @Before
    public void before() {
        setupFakeUserDir();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test
    public void testWithAllRunPhases() throws Exception {
        int threadCount = 2;
        TestWithAllRunPhases testInstance = new TestWithAllRunPhases();
        TestCase testCase = new TestCase(TEST_ID)
                .setProperty("threadCount", threadCount)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future runFuture = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(RUN);
                return null;
            }
        });
        sleepSeconds(5);
        testContext.stop();
        runFuture.get();

        container.invoke(TestPhase.LOCAL_TEARDOWN);

        System.out.println("done");

        assertEquals(threadCount, testInstance.beforeRunCount.get());
        assertEquals(threadCount, testInstance.afterRunCount.get());
        System.out.println(testInstance.timeStepCount);
    }


    public static class TestWithAllRunPhases {
        private final AtomicLong beforeRunCount = new AtomicLong();
        private final AtomicLong afterRunCount = new AtomicLong();
        private final AtomicLong timeStepCount = new AtomicLong();

        @BeforeRun
        public void beforeRun() {
            beforeRunCount.incrementAndGet();
        }

        @TimeStep
        public void timeStep() {
            timeStepCount.incrementAndGet();
        }

        @AfterRun
        public void afterRun() {
            afterRunCount.incrementAndGet();
        }
    }

    @Test
    public void testWithAllRunPhasesAndWarmup() throws Exception {
        int threadCount = 1;
        TestWithAllRunPhasesAndWarmup testInstance = new TestWithAllRunPhasesAndWarmup();
        TestCase testCase = new TestCase(TEST_ID)
                .setProperty("threadCount", threadCount)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future warmupFuture = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(WARMUP);
                return null;
            }
        });
        Thread.sleep(5000);
        testContext.stop();
        warmupFuture.get();
        container.invoke(TestPhase.LOCAL_AFTER_WARMUP);
        container.invoke(TestPhase.GLOBAL_PREPARE);

        Future runFuture = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(RUN);
                System.out.println("Done with run");
                return null;
            }
        });
        Thread.sleep(5000);
        testContext.stop();
        runFuture.get();

        container.invoke(TestPhase.LOCAL_TEARDOWN);

        assertEquals(1, testInstance.afterWarmupCalled.get());
        assertEquals(threadCount * 2, testInstance.states.size());
        for (TestWithAllRunPhasesAndWarmup.ThreadState state : testInstance.states.values()) {
            assertEquals(1, state.beforeRunCount);
            assertEquals(1, state.afterRunCount);
            assertTrue(state.timeStepCount > 1);
        }

        System.out.println("done");
    }

    public static class TestWithAllRunPhasesAndWarmup {

        final ConcurrentHashMap<Thread, ThreadState> states = new ConcurrentHashMap<Thread, ThreadState>();
        final AtomicLong afterWarmupCalled = new AtomicLong();

        @AfterWarmup(global = false)
        public void afterWarmup() {
            System.out.println("afterWarmup");
            afterWarmupCalled.incrementAndGet();
        }

        @BeforeRun
        public void beforeRun(ThreadState state) {
            System.out.println("beforeRun: " + Thread.currentThread() + " state: " + state);

            state.beforeRunCount++;

            if (states.putIfAbsent(Thread.currentThread(), state) != null) {
                throw new IllegalStateException();
            }
        }

        @TimeStep
        public void timeStep(ThreadState state) throws InterruptedException {
            System.out.println("timeStep: " + Thread.currentThread() + " state: " + state);

            Thread.sleep(1000);

            state.timeStepCount++;

            if (!states.containsKey(Thread.currentThread())) {
                throw new IllegalStateException();
            }
        }

        @AfterRun
        public void afterRun(ThreadState state) {
            System.out.println("afterRun: " + Thread.currentThread() + " state: " + state);

            state.afterRunCount++;

            if (!states.containsKey(Thread.currentThread())) {
                throw new IllegalStateException();
            }
        }

        public static class ThreadState {
            int beforeRunCount;
            int afterRunCount;
            int timeStepCount;
        }
    }

    @Test
    public void testWithThreadContext() throws Exception {
        int threadCount = 2;

        TestWithThreadState testInstance = new TestWithThreadState();

        TestCase testCase = new TestCase("someid")
                .setProperty("threadCount", threadCount)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future runFuture = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(RUN);
                return null;
            }
        });
        Thread.sleep(5000);
        testContext.stop();
        runFuture.get();

        container.invoke(TestPhase.LOCAL_TEARDOWN);

        assertEquals(threadCount, testInstance.map.size());

        // each context should be unique
        Set<BaseThreadState> contexts = new HashSet<BaseThreadState>(testInstance.map.values());
        assertEquals(threadCount, contexts.size());
    }

    public static class TestWithThreadState {

        private final Map<Thread, BaseThreadState> map = new ConcurrentHashMap<Thread, BaseThreadState>();

        @TimeStep
        public void timeStep(BaseThreadState state) {
            BaseThreadState found = map.get(Thread.currentThread());
            if (found == null) {
                map.put(Thread.currentThread(), state);
            } else if (found != state) {
                throw new RuntimeException("Unexpected context");
            }
        }
    }
}
