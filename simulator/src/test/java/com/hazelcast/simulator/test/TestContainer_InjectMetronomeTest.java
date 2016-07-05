package com.hazelcast.simulator.test;

import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import org.junit.Test;

import static com.hazelcast.simulator.test.DependencyInjector.METRONOME_INTERVAL_PROPERTY_NAME;
import static com.hazelcast.simulator.test.DependencyInjector.METRONOME_TYPE_PROPERTY_NAME;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.BUSY_SPINNING;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.NOP;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.SLEEPING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainer_InjectMetronomeTest extends AbstractTestContainerTest {

    @Test
    public void testConstructor_withTestcase() throws Exception {
        TestCase testCase = new TestCase("TestContainerMetronomeTest");
        testCase.setProperty("class", MetronomeTest.class.getName());
        testCase.setProperty("threadCount", "1");
        testCase.setProperty(METRONOME_INTERVAL_PROPERTY_NAME, "100");
        testCase.setProperty(METRONOME_TYPE_PROPERTY_NAME, SLEEPING.name());

        testContainer = new TestContainer(testContext, testCase);
        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(testContainer.getTestInstance());
        assertTrue(testContainer.getTestInstance() instanceof MetronomeTest);

        MetronomeTest metronomeTest = (MetronomeTest) testContainer.getTestInstance();
        assertNotNull(metronomeTest.workerMetronome);
        assertEquals(100, metronomeTest.workerMetronome.getInterval());
        assertEquals(SLEEPING, metronomeTest.workerMetronome.getType());
    }

    @Test
    public void testInjectMetronome() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.nopMetronome);
        assertEquals(0, test.nopMetronome.getInterval());
        assertEquals(NOP, test.nopMetronome.getType());
    }

    @Test
    public void testInjectMetronome_withParameters() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.metronome);
        assertEquals(200, test.metronome.getInterval());
        assertEquals(BUSY_SPINNING, test.metronome.getType());
    }

    @Test
    public void testInjectMetronome_withoutAnnotation() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedMetronome);
    }

    @SuppressWarnings("WeakerAccess")
    static class MetronomeTest {

        @InjectMetronome
        private Metronome nopMetronome;

        @InjectMetronome(intervalMillis = 200, type = BUSY_SPINNING)
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
