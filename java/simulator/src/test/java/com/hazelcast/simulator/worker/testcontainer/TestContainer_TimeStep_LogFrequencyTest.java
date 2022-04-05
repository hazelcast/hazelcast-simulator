package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.fake.FakeInstance;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_LogFrequencyTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        LogFrequencyTest testInstance = new LogFrequencyTest();
        TestCase testCase = new TestCase("test")
                .setProperty("logFrequency", 1000)
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
        TestContainer container = new TestContainer(testContext, testInstance, testCase, mock(FakeInstance.class));

        for (TestPhase phase : TestPhase.values()) {
            container.invoke(phase);
        }
    }

    public static class LogFrequencyTest extends AbstractTest {
        @TimeStep
        public void timeStep() {
            throw new StopException();
        }

    }
}
