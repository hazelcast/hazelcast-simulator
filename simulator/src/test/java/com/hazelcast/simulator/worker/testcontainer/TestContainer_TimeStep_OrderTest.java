package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_OrderTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        TestInstance testInstance = new TestInstance();
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
        assertNoExceptions();
        assertEquals(asList("beforeRun", "timeStep", "afterRun"), testInstance.calls);
    }

    public static class TestInstance {
        private final List<String> calls = new LinkedList<String>();

        @BeforeRun
        public void beforeRun() {
            calls.add("beforeRun");
        }

        @TimeStep
        public void timeStep() {
            calls.add("timeStep");
            throw new StopException();
        }

        @AfterRun
        public void afterRun() {
            calls.add("afterRun");
        }
    }
}
