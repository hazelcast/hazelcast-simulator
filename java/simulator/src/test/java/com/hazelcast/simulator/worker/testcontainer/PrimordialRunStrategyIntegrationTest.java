package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.mock;

public class PrimordialRunStrategyIntegrationTest {

    @Before
    public void setup() {
        setupFakeUserDir();
    }

    @AfterRun
    public void after() {
        teardownFakeUserDir();
    }

    @Test
    public void testWithAllPhases() throws Exception {
        int threadCount = 2;
        DummyTest testInstance = new DummyTest();
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

        assertTrue(testInstance.count.get() > 1000);
    }


    public static class DummyTest {
        @InjectTestContext
        public TestContext testContext;
        public int threadCount;
        private final AtomicLong count = new AtomicLong();

        @Run
        public void run() {
            ThreadSpawner spawner = new ThreadSpawner("id");
            for (int k = 0; k < threadCount; k++) {
                spawner.spawn(new Runnable() {
                    @Override
                    public void run() {
                        while (!testContext.isStopped()) {
                            count.incrementAndGet();
                        }
                    }
                });
            }
        }
    }

}
