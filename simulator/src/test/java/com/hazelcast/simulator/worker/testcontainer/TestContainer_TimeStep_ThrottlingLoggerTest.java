package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_ThrottlingLoggerTest extends TestContainer_AbstractTest{

    @Test
    public void test() throws Exception {
        ThrottlingLoggerTest testInstance = new ThrottlingLoggerTest();
        TestCase testCase = new TestCase("exceptionTest")
                .setProperty("logRateMs", 1000)
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);

        for (TestPhase phase : TestPhase.values()) {
            container.invoke(phase);
        }
    }

    public static class ThrottlingLoggerTest extends AbstractTest {
        @TimeStep
        public void timeStep() {
            throw new StopException();
        }

    }
}
