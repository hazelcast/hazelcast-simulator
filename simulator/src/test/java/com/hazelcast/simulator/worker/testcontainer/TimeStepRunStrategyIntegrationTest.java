package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TimeStepRunStrategyIntegrationTest {

    private static final String TEST_ID = "SomeId";

    @Before
    public void before() {
        setupFakeUserDir();
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test
    public void testWithAllRunPhases() throws Exception {
        int threadCount = 2;
        TestWithAllRunPhases testInstance = new TestWithAllRunPhases();
        TestCase testCase = new TestCase(TEST_ID)
                .setProperty("threadCount", threadCount)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
        final TestContainer container = new TestContainer(testContext, testInstance, testCase);
        container.invoke(SETUP);

        Future runFuture = spawn(new Callable() {
            @Override
            public Object call() throws Exception {
                container.invoke(RUN);
                return null;
            }
        });
        sleepSeconds(5);
        testContext.stop();
        runFuture.get();

        container.invoke(TestPhase.LOCAL_TEARDOWN);

        System.out.println("done");

        assertEquals(threadCount, testInstance.beforeRunCount.get());
        assertEquals(threadCount, testInstance.afterRunCount.get());
        System.out.println(testInstance.timeStepCount);
    }


    public static class TestWithAllRunPhases {
        private final AtomicLong beforeRunCount = new AtomicLong();
        private final AtomicLong afterRunCount = new AtomicLong();
        private final AtomicLong timeStepCount = new AtomicLong();

        @BeforeRun
        public void beforeRun() {
            beforeRunCount.incrementAndGet();
        }

        @TimeStep
        public void timeStep() {
            timeStepCount.incrementAndGet();
        }

        @AfterRun
        public void afterRun() {
            afterRunCount.incrementAndGet();
        }
    }

    @Test
    public void testWithThreadContext() throws Exception {
        int threadCount = 2;

        TestWithThreadState testInstance = new TestWithThreadState();

        TestCase testCase = new TestCase("someid")
                .setProperty("threadCount", threadCount)
                .setProperty("class", testInstance.getClass());

        TestContextImpl testContext = new TestContextImpl(
                mock(HazelcastInstance.class), testCase.getId(), "localhost", mock(WorkerConnector.class));
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

        assertEquals(threadCount, testInstance.map.size());

        // each context should be unique
        Set<BaseThreadState> contexts = new HashSet<BaseThreadState>(testInstance.map.values());
        assertEquals(threadCount, contexts.size());
    }

    public static class TestWithThreadState {

        private final Map<Thread, BaseThreadState> map = new ConcurrentHashMap<Thread, BaseThreadState>();

        @TimeStep
        public void timeStep(BaseThreadState state) {
            BaseThreadState found = map.get(Thread.currentThread());
            if (found == null) {
                map.put(Thread.currentThread(), state);
            } else if (found != state) {
                throw new RuntimeException("Unexpected context");
            }
        }
    }
}
