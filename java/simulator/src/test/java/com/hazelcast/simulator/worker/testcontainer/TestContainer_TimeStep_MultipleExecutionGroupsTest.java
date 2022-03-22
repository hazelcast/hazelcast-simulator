package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.Server;
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
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static com.hazelcast.simulator.utils.TestUtils.assertNoExceptions;
import static java.util.Collections.disjoint;
import static java.util.Collections.synchronizedSet;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TestContainer_TimeStep_MultipleExecutionGroupsTest extends TestContainer_AbstractTest {

    @Test
    public void test() throws Exception {
        MultipleExecutionGroupsTest testInstance = new MultipleExecutionGroupsTest();
        TestCase testCase = new TestCase("multipleExecutionGroupsTest")
                .setProperty("group1ThreadCount", 2)
                .setProperty("group2ThreadCount", 3)
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
            group1Threads.add(Thread.currentThread());

            for (; ; ) {
                long current = group1Counter.get();
                if (current == 0) {
                    throw new StopException();
                }

                if (group1Counter.compareAndSet(current, current - 1)) {
                    break;
                }
            }
        }

        @TimeStep(executionGroup = "group2")
        public void group2TimeStep() {
            group2Threads.add(Thread.currentThread());

            for (; ; ) {
                long current = group2Counter.get();
                if (current == 0) {
                    throw new StopException();
                }

                if (group2Counter.compareAndSet(current, current - 1)) {
                    break;
                }
            }
        }
    }
}
