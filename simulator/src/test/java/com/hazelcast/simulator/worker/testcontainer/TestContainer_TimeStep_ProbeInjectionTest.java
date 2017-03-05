package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.EmptyProbe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.TestSupport.assertInstanceOf;
import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static org.mockito.Mockito.mock;

/**
 * Test that verifies that the timestep methods can deal correctly with an injected probe
 */
public class TestContainer_TimeStep_ProbeInjectionTest extends TestContainer_AbstractTest {

    @Test
    public void testMeasureLatency() throws Exception {
        TestClass testInstance = new TestClass();
        TestCase testCase = new TestCase("exceptionTest")
                .setProperty("threadCount", 1)
                .setProperty("measureLatency", true)
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
        assertInstanceOf(HdrProbe.class, testInstance.probe);
    }

    @Test
    public void testNoMeasureLatency() throws Exception {
        TestClass testInstance = new TestClass();
        TestCase testCase = new TestCase("exceptionTest")
                .setProperty("threadCount", 1)
                .setProperty("measureLatency", false)
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
        assertInstanceOf(EmptyProbe.class, testInstance.probe);
    }

    public static class TestClass {
        volatile Probe probe;

        @TimeStep
        public void test(Probe probe, @StartNanos long startNanos) {
            this.probe = probe;
            throw new StopException();
        }
    }
}

