package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.BindException;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_UnusedProbabilityTest extends TestContainer_AbstractTest {

    @Test(expected = IllegalTestException.class)
    public void test() throws Exception {
        UnusedProbabilityTest testInstance = new UnusedProbabilityTest();
        TestCase testCase = new TestCase("stopRun")
                .setProperty("threadCount", 1)
                .setProperty("iterations", 100)
                .setProperty("unusedProb", "0.2")
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
        TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);
    }

    public static class UnusedProbabilityTest {
        private final AtomicLong runCount = new AtomicLong(0);

        @TimeStep
        public void timeStep() {
            runCount.incrementAndGet();
        }
    }
}