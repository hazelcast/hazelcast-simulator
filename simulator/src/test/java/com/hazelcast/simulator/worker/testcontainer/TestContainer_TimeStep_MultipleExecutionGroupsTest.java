package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.TestSupport.spawn;
import static com.hazelcast.simulator.common.TestPhase.RUN;
import static com.hazelcast.simulator.common.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;
import static java.util.Collections.disjoint;
import static java.util.Collections.synchronizedSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class TestContainer_TimeStep_MultipleExecutionGroupsTest extends TestContainer_AbstractTest {

    @Test
    public void testWithAllPhases() throws Exception {
        MultipleExecutionGroupsTest testInstance = new MultipleExecutionGroupsTest();
        TestCase testCase = new TestCase("multipleExecutionGroupsTest")
                .setProperty("group1ThreadCount", 2)
                .setProperty("group2ThreadCount", 3)
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
        assertEquals(2, testInstance.group1Threads.size());
        assertEquals(3, testInstance.group2Threads.size());
        assertTrue(disjoint(testInstance.group1Threads, testInstance.group2Threads));
    }

    public static class MultipleExecutionGroupsTest {
        private final AtomicLong group1Counter = new AtomicLong(1000);
        private final AtomicLong group2Counter = new AtomicLong(2000);
        private final Set<Thread> group1Threads = synchronizedSet(new HashSet<Thread>());
        private final Set<Thread> group2Threads = synchronizedSet(new HashSet<Thread>());

        @TimeStep(executionGroup = "group1")
        public void group1TimeStep() {
            if (group1Counter.get() == 0) {
                throw new StopException();
            }

            group1Counter.decrementAndGet();
            group1Threads.add(Thread.currentThread());
        }

        @TimeStep(executionGroup = "group2")
        public void group2TimeStep() {
            if (group2Counter.get() == 0) {
                throw new StopException();
            }

            group2Counter.decrementAndGet();
            group2Threads.add(Thread.currentThread());
        }
    }
}
