package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStepReturnTypesTest extends TestContainer_AbstractTest {

    @Test
    public void testWithAllPhases() throws Exception {
        int threadCount = 2;
        TestWithAllTimeStepPhases testInstance = new TestWithAllTimeStepPhases();
        TestCase testCase = new TestCase("id")
                .setProperty("threadCount", threadCount)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                testCase.getId(), "localhost", mock(Server.class));
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

        assertEquals(threadCount, testInstance.beforeRunCount.get());
        assertEquals(threadCount, testInstance.afterRunCount.get());
        //assertTrue(testInstance.timeStepCount.get() > 100);
    }


    public static class TestWithAllTimeStepPhases {
        private final AtomicLong beforeRunCount = new AtomicLong();
        private final AtomicLong afterRunCount = new AtomicLong();
        private final AtomicLong timeStepCount = new AtomicLong();

        @BeforeRun
        public void beforeRun() {
            beforeRunCount.incrementAndGet();
        }

        @TimeStep(prob = -1)
        public boolean returnBoolean() {
            return false;
        }

        @TimeStep(prob = 0.1)
        public int returnInt() {
            return 1;
        }

        @TimeStep(prob = 0.1)
        public char returnChar() {
            return 'c';
        }

        @TimeStep(prob = 0.1)
        public short returnShort() {
            return (short)0;
        }

        @TimeStep(prob = 0.1)
        public long returnLong() {
            return 1L;
        }

        @TimeStep(prob = 0.1)
        public double returnDouble() {
            return 0;
        }

        @TimeStep(prob = 0.1)
        public float returnFloat() {
            return 0;
        }

        @TimeStep(prob = 0.1)
        public String returnObject() {
            return "";
        }

        @AfterRun
        public void afterRun() {
            afterRunCount.incrementAndGet();
        }
    }

}
