package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import org.junit.Test;

import static com.hazelcast.simulator.worker.metronome.MetronomeType.NOP;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.SLEEPING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestContainer_InjectMetronomeTest extends AbstractTestContainerTest {

    @Test
    public void testConstructor_withTestcase() throws Exception {
        TestCase testCase = new TestCase("TestContainerMetronomeTest")
                .setProperty("class", MetronomeTest.class.getName())
                .setProperty("threadCount", 1)
                .setProperty("metronomeIntervalUs", 100000)
                .setProperty("metronomeType", SLEEPING.name());

        testContainer = new TestContainer(testContext, testCase);
        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        MetronomeTest metronomeTest = (MetronomeTest) testContainer.getTestInstance();
        assertNotNull(metronomeTest.workerMetronome);
        assertEquals(100, metronomeTest.workerMetronome.getInterval());
        assertEquals(SLEEPING, metronomeTest.workerMetronome.getType());
    }

    @Test
    public void testInjectMetronome() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.metronome);
        assertEquals(0, test.metronome.getInterval());
        assertEquals(NOP, test.metronome.getType());
    }


    @Test
    public void testInjectMetronome_withoutAnnotation() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedMetronome);
    }

    @SuppressWarnings("WeakerAccess")
    public static class MetronomeTest {

        @InjectMetronome
        private Metronome metronome;

        @SuppressWarnings("unused")
        private Metronome notAnnotatedMetronome;

        private Metronome workerMetronome;

        @RunWithWorker
        public Worker createWorker() {
            return new Worker();
        }

        private class Worker extends AbstractMonotonicWorker {

            @Override
            protected void timeStep() throws Exception {
                workerMetronome = getWorkerMetronome();
                stopTestContext();
            }
        }
    }

    @Test(expected = IllegalTestException.class)
    public void testInjectMetronome_withIllegalFieldType() {
        IllegalFieldTypeTest test = new IllegalFieldTypeTest();
        testContainer = createTestContainer(test);
    }

    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectMetronome
        private Object noProbeField;
    }
}
