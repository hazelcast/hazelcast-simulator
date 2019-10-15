package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.HdrHistogram.Recorder;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_AsyncSupportTest extends TestContainer_AbstractTest {

    @Test
    public void testWithoutMetronome_withSingleAsyncMethod() throws Exception {
        StartAsyncTest_withSingleAsyncMethod testInstance = new StartAsyncTest_withSingleAsyncMethod();
        int totalIterationCount = 50;
        TestContainer container = createContainerAndRunTestInstance(testInstance, totalIterationCount);

        long timeStepCount = getProbeTotalCount("timeStep", container);
        assertProbeTotalCount("asyncTimeStep", (totalIterationCount - timeStepCount), container);
    }

    @Test
    public void testWithoutMetronome_withMultipleAsyncMethod() throws Exception {
        StartAsyncTest_withMultipleAsyncMethod testInstance = new StartAsyncTest_withMultipleAsyncMethod();
        int totalIterationCount = 1000;
        TestContainer container = createContainerAndRunTestInstance(testInstance, totalIterationCount);

        long asyncTimeStep1 = getProbeTotalCount("asyncTimeStep1", container);
        long asyncTimeStep2 = getProbeTotalCount("asyncTimeStep2", container);

        assertTrue(asyncTimeStep1 > 0);
        assertTrue(asyncTimeStep2 > 0);
        assertEquals(totalIterationCount, asyncTimeStep1 + asyncTimeStep2);
    }

    private TestContainer createContainerAndRunTestInstance(Object testInstance, int totalIterationCount) throws Exception {
        TestCase testCase = new TestCase("test")
                .setProperty("iterations", totalIterationCount)
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
        TestContainer container = new TestContainer(testContext, testInstance, testCase);

        for (TestPhase phase : TestPhase.values()) {
            container.invoke(phase);
        }
        return container;
    }

    private static void assertProbeTotalCount(String probeName, long count, TestContainer container) {
        long totalCount = getProbeTotalCount(probeName, container);
        assertEquals(count, totalCount);
    }

    private static long getProbeTotalCount(String probeName, TestContainer container) {
        Map<String, Probe> probeMap = container.getProbeMap();
        HdrProbe probe = (HdrProbe) probeMap.get(probeName);
        Recorder recorder = probe.getRecorder();
        return recorder.getIntervalHistogram().getTotalCount();
    }

    public static class StartAsyncTest_withSingleAsyncMethod {
        @TimeStep(prob = 0.5)
        public void timeStep() {

        }

        @TimeStep(prob = 0.5)
        public CompletableFuture<Object> asyncTimeStep() {
            return new CompletableFuture<>();
        }
    }

    public static class StartAsyncTest_withMultipleAsyncMethod {
        @TimeStep(prob = 0.5)
        public CompletableFuture<Object> asyncTimeStep1() {
            return new CompletableFuture();
        }

        @TimeStep(prob = 0.5)
        public CompletableFuture<Object> asyncTimeStep2() {
            return new CompletableFuture();
        }
    }

}
