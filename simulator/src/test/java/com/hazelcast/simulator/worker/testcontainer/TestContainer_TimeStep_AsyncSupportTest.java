package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.HdrHistogram.Recorder;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_AsyncSupportTest extends TestContainer_AbstractTest {

    @Test
    public void testWithoutMetronome() throws Exception {
        TestContainer_TimeStep_AsyncSupportTest.StartAsyncTest testInstance = new TestContainer_TimeStep_AsyncSupportTest.StartAsyncTest();
        int totalIterationCount = 50;
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

        long timeStepCount = getProbeTotalCount("timeStep", container);
        assertProbeTotalCount("asyncTimeStep", (totalIterationCount - timeStepCount), container);

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

    public static class StartAsyncTest  {
        @TimeStep(prob = 0.5)
        public void timeStep() {

        }

        @TimeStep(prob = 0.5)
        public ICompletableFuture<Object> asyncTimeStep() {
            return new DummyICompletableFuture();
        }
    }

    private static class DummyICompletableFuture implements ICompletableFuture<Object> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public void andThen(ExecutionCallback<Object> executionCallback) {
            executionCallback.onResponse(null);
        }

        @Override
        public void andThen(ExecutionCallback<Object> executionCallback, Executor executor) {
            throw new UnsupportedOperationException("not implemented");
        }
    }
}
