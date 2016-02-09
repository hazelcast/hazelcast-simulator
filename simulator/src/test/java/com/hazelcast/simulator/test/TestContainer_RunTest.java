package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestContainer_RunTest extends AbstractTestContainerTest {

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
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("threadCount", "0");
        testCase = new TestCase("TestContainerTest", properties);

        RunWithWorkerTest test = new RunWithWorkerTest();
        testContainer = createTestContainer(test);

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
        final RunWithIWorkerTest test = new RunWithIWorkerTest();
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

        volatile boolean runWithWorkerCalled;

        @RunWithWorker
        IWorker createWorker() {
            return new IWorker() {

                @Override
                public void run() {
                    runWithWorkerCalled = true;
                }

                @Override
                public void afterCompletion() {
                }
            };
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
