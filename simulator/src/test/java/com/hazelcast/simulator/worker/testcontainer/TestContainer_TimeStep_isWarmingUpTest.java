package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.common.TestPhase.GLOBAL_AFTER_WARMUP;
import static com.hazelcast.simulator.common.TestPhase.LOCAL_AFTER_WARMUP;
import static com.hazelcast.simulator.common.TestPhase.WARMUP;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_isWarmingUpTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        WarmupTest testInstance = new WarmupTest();
        TestCase testCase = new TestCase("exceptionTest")
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass().getName());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);

        for (TestPhase phase : TestPhase.values()) {
            container.invoke(phase);
        }

        assertEquals(asList(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE), testInstance.warmupList);
    }

    @Test
    public void testWithoutWarmup() throws Exception {
        WarmupTest testInstance = new WarmupTest();
        TestCase testCase = new TestCase("exceptionTest")
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass().getName());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);

        for (TestPhase phase : TestPhase.values()) {
            if (phase == WARMUP || phase == LOCAL_AFTER_WARMUP || phase == GLOBAL_AFTER_WARMUP) {
                continue;
            }
            container.invoke(phase);
        }

        assertEquals(asList(FALSE, FALSE, FALSE), testInstance.warmupList);
    }

    public static class WarmupTest extends AbstractTest {
        final List<Boolean> warmupList = new LinkedList<Boolean>();

        @BeforeRun
        public void beforeRun() {
            warmupList.add(testContext.isWarmingUp());
        }

        @TimeStep
        public void timeStep() {
            warmupList.add(testContext.isWarmingUp());
            throw new StopException();
        }

        @AfterRun
        public void afterRun() {
            warmupList.add(testContext.isWarmingUp());
        }
    }
}
