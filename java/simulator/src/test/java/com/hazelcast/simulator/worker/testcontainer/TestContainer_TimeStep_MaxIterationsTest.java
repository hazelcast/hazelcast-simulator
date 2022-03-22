package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests of the {@link StopException} works correctly.
 */
public class TestContainer_TimeStep_MaxIterationsTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        MaxIterationTest testInstance = new MaxIterationTest();
        TestCase testCase = new TestCase("stopRun")
                .setProperty("threadCount", 1)
                .setProperty("iterations", 100)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        for (TestPhase phase : TestPhase.values()) {
            container.invoke(phase);
        }

        assertNoExceptions();
        assertEquals(100, testInstance.runCount.get());
    }

    public static class MaxIterationTest {
        private final AtomicLong runCount = new AtomicLong(0);

        @TimeStep
        public void timeStep() {
            runCount.incrementAndGet();
        }
    }
}
