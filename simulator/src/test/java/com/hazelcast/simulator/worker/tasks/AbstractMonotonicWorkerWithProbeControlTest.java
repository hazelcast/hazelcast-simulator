/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.testcontainer.TestContainer;
import com.hazelcast.simulator.worker.testcontainer.TestContextImpl;
import org.HdrHistogram.Histogram;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AbstractMonotonicWorkerWithProbeControlTest {

    private static final int THREAD_COUNT = 3;
    private static final int ITERATION_COUNT = 10;
    private static final int DEFAULT_TEST_TIMEOUT = 30000;

    private enum Operation {
        STOP_TEST_CONTEXT,
        USE_MANUAL_PROBE
    }

    private WorkerTest test;
    private TestContextImpl testContext;
    private TestContainer testContainer;

    @Before
    public void before() {
        setupFakeUserDir();

        test = new WorkerTest();
        testContext = new TestContextImpl(mock(HazelcastInstance.class), "Test", "localhost", mock(WorkerConnector.class));
        testContainer = new TestContainer(testContext, test, new TestCase("foo").setProperty("threadCount", THREAD_COUNT));

        ExceptionReporter.reset();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testInvokeSetup() throws Exception {
        testContainer.invoke(TestPhase.SETUP);

        assertEquals(testContext, test.testContext);
        assertEquals(0, test.workerCreated.get());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testRun() throws Exception {
        test.operation = Operation.STOP_TEST_CONTEXT;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.testContext.isStopped());
        assertEquals(THREAD_COUNT, test.workerCreated.get());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testTimeStep_shouldUseProbe() throws Exception {
        test.operation = Operation.USE_MANUAL_PROBE;

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(test.probe);
        Histogram intervalHistogram = ((HdrProbe) test.probe).getIntervalHistogram();
        assertEquals(THREAD_COUNT * ITERATION_COUNT, intervalHistogram.getTotalCount());
        assertEquals(THREAD_COUNT, test.workerCreated.get());
    }

    @SuppressWarnings("deprecation")
    public static class WorkerTest {

        private TestContext testContext;

        private final AtomicInteger workerCreated = new AtomicInteger();

        private volatile Operation operation;
        private volatile Probe probe;

        @Setup
        public void setup(TestContext testContext) {
            this.testContext = testContext;
        }

        @RunWithWorker
        public Worker createWorker() {
            workerCreated.getAndIncrement();
            return new Worker(this);
        }

        private class Worker extends AbstractMonotonicWorkerWithProbeControl {

            private final WorkerTest test;

            Worker(WorkerTest test) {
                this.test = test;
            }

            @Override
            protected void timeStep(Probe probe) {
                switch (operation) {
                    case STOP_TEST_CONTEXT:
                        stopTestContext();
                        break;
                    case USE_MANUAL_PROBE:
                        long started = System.nanoTime();
                        if (getIteration() == ITERATION_COUNT) {
                            test.probe = probe;
                            stopWorker();
                            break;
                        }
                        probe.recordValue(System.nanoTime() - started);
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported operation: " + operation);
                }
            }
        }
    }
}
