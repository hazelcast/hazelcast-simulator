package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;

/**
 * Tests of the {@link StopException} works correctly.
 */
public class TestContainer_TimeStep_StopTest extends TestContainer_AbstractTest {

    @Test
    public void testWithAllPhases() throws Exception {
        TimeStepStopTest testInstance = new TimeStepStopTest();
        TestCase testCase = new TestCase("stopTest")
                .setProperty("threadCount", 1)
                .setProperty("class", testInstance.getClass().getName());

        TestContextImpl testContext = new TestContextImpl(testCase.getId());
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future runFuture = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(RUN);
                return null;
            }
        });
        Thread.sleep(5000);
        testContext.stop();
        runFuture.get();

        container.invoke(TestPhase.LOCAL_TEARDOWN);

        assertNoExceptions();
    }

    public static class TimeStepStopTest {
        private final AtomicLong counter = new AtomicLong(1000);

        @TimeStep
        public void timeStep() {
            if (counter.get() == 0) {
                throw new StopException();
            }

            counter.decrementAndGet();
        }
    }
}
