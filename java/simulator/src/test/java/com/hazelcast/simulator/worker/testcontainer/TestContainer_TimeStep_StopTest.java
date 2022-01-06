package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;
import static org.mockito.Mockito.mock;

/**
 * Tests of the {@link StopException} works correctly.
 */
public class TestContainer_TimeStep_StopTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        TimeStepStopTest testInstance = new TimeStepStopTest();
        TestCase testCase = new TestCase("stopRun")
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
