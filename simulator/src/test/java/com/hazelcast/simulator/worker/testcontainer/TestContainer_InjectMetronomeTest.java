/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.metronome.EmptyMetronome;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.SleepingMetronome;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import org.junit.Test;

import static com.hazelcast.simulator.TestSupport.assertInstanceOf;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestContainer_InjectMetronomeTest extends TestContainer_AbstractTest {

    @Test
    public void testConstructor_withTestcase() throws Exception {
        TestCase testCase = new TestCase("TestContainerMetronomeTest")
                .setProperty("class", MetronomeTest.class)
                .setProperty("threadCount", 1)
                .setProperty("interval", "5us")
                .setProperty("metronomeClass", SleepingMetronome.class);

        testContainer = new TestContainer(testContext, testCase);
        MetronomeTest testInstance = (MetronomeTest) testContainer.getTestInstance();

        SleepingMetronome metronome = assertInstanceOf(SleepingMetronome.class, testInstance.metronome);
        assertEquals(MICROSECONDS.toNanos(5), metronome.getIntervalNanos());

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(metronome);
    }

    @Test
    public void testInjectMetronome() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNotNull(test.metronome);
        Metronome metronome = test.metronome;
        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testInjectMetronome_withWorkerMetronome() throws Exception {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        testContainer.invoke(TestPhase.SETUP);
        testContainer.invoke(TestPhase.RUN);

        assertNotNull(test.workerMetronome);
        Metronome metronome = test.workerMetronome;
        assertTrue(metronome instanceof EmptyMetronome);
    }

    @Test
    public void testInjectMetronome_withoutAnnotation() {
        MetronomeTest test = new MetronomeTest();
        testContainer = createTestContainer(test);

        assertNull(test.notAnnotatedMetronome);
    }

    @SuppressWarnings({"WeakerAccess", "deprecation"})
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

            @TimeStep
            protected void timeStep() {
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

    @SuppressWarnings("deprecation")
    private static class IllegalFieldTypeTest extends BaseTest {

        @InjectMetronome
        private Object noProbeField;
    }
}
