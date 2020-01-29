package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.internal.util.executor.CompletableFutureTask;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_AsyncSupportTest extends TestContainer_AbstractTest {
    @Test
    public void test() throws Exception {
        AsyncTest testInstance = new AsyncTest();
        TestCase testCase = new TestCase("stopRun")
                .setProperty("iterations",100)
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future f = spawn((Callable) () -> {
            container.invoke(RUN);
            return null;
        });

        assertCompletesEventually(f);
        assertNoExceptions();
        assertTrueEventually(() -> assertEquals(100, testInstance.asyncCount));
    }


    public static class AsyncTest {
        public ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        public volatile int asyncCount;

        @TimeStep
        public CompletableFuture<Object> asyncTimeStep() {
            CompletableFuture completableFuture = new CompletableFuture();
            scheduler.schedule(() -> {
                asyncCount++;
                completableFuture.complete("1");
            },1000, TimeUnit.MILLISECONDS);
            return completableFuture;
        }
    }
}
