package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.junit.Test;

import java.util.Map;

import static com.hazelcast.simulator.test.TestContainer_RunTest.MultiProbeWorkerTest.Operation.FIRST_OPERATION;
import static com.hazelcast.simulator.test.TestContainer_RunTest.MultiProbeWorkerTest.Operation.SECOND_OPERATION;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_RunTest extends AbstractTestContainerTest {

    private static final int THREAD_COUNT = 3;
    private static final int ITERATION_COUNT = 10;

    @Test
    public void testRun() throws Exception {
        BaseTest test = new BaseTest();
        testContainer = createTestContainer(test);

        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.runCalled);
    }

    @Test
    public void testRunWithWorker() throws Exception {
        final RunWithWorkerTest test = new RunWithWorkerTest();
        testContainer = createTestContainer(test);
        Thread testStopper = new Thread() {
            @Override
            public void run() {
                while (!test.runWithWorkerCalled) {
                    sleepMillis(50);
                }
                testContext.stop();
            }
        };

        testStopper.start();
        testContainer.invoke(TestPhase.RUN);
        testStopper.join();

        assertTrue(test.runWithWorkerCreated);
        assertTrue(test.runWithWorkerCalled);
    }

    @Test
    public void testRunWithWorker_withThreadCountZero() throws Exception {
        RunWithWorkerTest test = new RunWithWorkerTest();
        testContainer = new TestContainer(testContext, test, 0);

        testContainer.invoke(TestPhase.RUN);

        assertFalse(test.runWithWorkerCreated);
        assertFalse(test.runWithWorkerCalled);
    }

    private static class RunWithWorkerTest {

        volatile boolean runWithWorkerCreated;
        volatile boolean runWithWorkerCalled;

        @RunWithWorker
        IWorker createWorker() {
            runWithWorkerCreated = true;

            return new AbstractMonotonicWorker() {

                @Override
                protected void timeStep() throws Exception {
                    runWithWorkerCalled = true;
                }
            };
        }
    }

    @Test
    public void testRunWithWorker_withLocalIWorkerImplementation() throws Exception {
        final RunWithIWorkerTest test = new RunWithIWorkerTest(testContext.getTargetInstance());
        testContainer = createTestContainer(test);
        Thread testStopper = new Thread() {
            @Override
            public void run() {
                while (!test.runWithWorkerCalled) {
                    sleepMillis(50);
                }
                testContext.stop();
            }
        };

        testStopper.start();
        testContainer.invoke(TestPhase.RUN);
        testStopper.join();

        assertTrue(test.runWithWorkerCalled);
    }

    private static class RunWithIWorkerTest {

        private final HazelcastInstance hazelcastInstance;

        volatile boolean runWithWorkerCalled;

        RunWithIWorkerTest(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @RunWithWorker
        IWorker createWorker() {
            return new IWorker() {

                @InjectHazelcastInstance
                HazelcastInstance injectedHazelcastInstance;

                @Override
                public void run() {
                    runWithWorkerCalled = true;

                    assertEquals(hazelcastInstance, injectedHazelcastInstance);
                }

                @Override
                public void afterCompletion() {
                }
            };
        }
    }

    @Test
    public void testRunWithWorker_withAbstractWorkerWithMultipleProbesWorker() throws Exception {
        MultiProbeWorkerTest test = new MultiProbeWorkerTest();
        testContainer = new TestContainer(testContext, test, THREAD_COUNT);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertTrue(test.runWithWorkerCreated);
        assertTrue(test.runWithWorkerCalled);

        Map<String, Probe> probeMap = testContainer.getProbeMap();
        assertEquals(MultiProbeWorkerTest.Operation.values().length, probeMap.size());

        long totalCount = 0;
        for (Probe probe : probeMap.values()) {
            totalCount += probe.getIntervalHistogram().getTotalCount();
        }
        assertEquals(THREAD_COUNT * ITERATION_COUNT, totalCount);
    }

    static class MultiProbeWorkerTest {

        enum Operation {
            FIRST_OPERATION,
            SECOND_OPERATION
        }

        private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

        volatile boolean runWithWorkerCreated;
        volatile boolean runWithWorkerCalled;

        @Setup
        public void setup() {
            operationSelectorBuilder
                    .addOperation(FIRST_OPERATION, 0.5)
                    .addDefaultOperation(SECOND_OPERATION);
        }

        @RunWithWorker
        IWorker createWorker() {
            runWithWorkerCreated = true;

            return new Worker(operationSelectorBuilder, this);
        }

        private static class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

            private MultiProbeWorkerTest test;

            public Worker(OperationSelectorBuilder<Operation> operationSelectorBuilder, MultiProbeWorkerTest test) {
                super(operationSelectorBuilder);
                this.test = test;
            }

            @Override
            protected void timeStep(Operation operation, Probe probe) throws Exception {
                test.runWithWorkerCalled = true;
                if (getIteration() == ITERATION_COUNT) {
                    stopWorker();
                    return;
                }
                probe.recordValue(randomInt(5000));
            }
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testRun_withMissingAnnotation() {
        createTestContainer(new MissingRunAnnotationTest());
    }

    private static class MissingRunAnnotationTest {
    }

    @Test(expected = IllegalTestException.class)
    public void testRun_withDuplicateRunAnnotations() {
        createTestContainer(new DuplicateRunAnnotationsTest());
    }

    private static class DuplicateRunAnnotationsTest {

        @Run
        public void run1() {
        }

        @Run
        public void run2() {
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testRun_withDuplicateRunWithWorkerAnnotations() {
        createTestContainer(new DuplicateRunWithWorkerAnnotationsTest());
    }

    private static class DuplicateRunWithWorkerAnnotationsTest {

        @RunWithWorker
        public IWorker createWorker1() {
            return null;
        }

        @RunWithWorker
        public IWorker createWorker2() {
            return null;
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testRun_withDuplicateMixedAnnotations() {
        createTestContainer(new DuplicateMixedRunAnnotationsTest());
    }

    private static class DuplicateMixedRunAnnotationsTest {

        @Run
        public void run() {
        }

        @RunWithWorker
        public IWorker createWorker() {
            return null;
        }
    }
}
