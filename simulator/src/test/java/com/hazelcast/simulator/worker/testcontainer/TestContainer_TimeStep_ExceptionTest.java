package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static com.hazelcast.simulator.utils.TestUtils.assertException;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_ExceptionTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        ExceptionTest testInstance = new ExceptionTest();
        TestCase testCase = new TestCase("exceptionTest")
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future f = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(RUN);
                return null;
            }
        });

        assertCompletesEventually(f);
        assertEquals(1L, testInstance.counter.get());
        assertException("IllegalStateException", 1);
    }

    public static class ExceptionTest {
        final AtomicLong counter = new AtomicLong(0);

        @TimeStep
        public void timeStep() {
            counter.incrementAndGet();
            throw new IllegalStateException();
        }
    }
}
