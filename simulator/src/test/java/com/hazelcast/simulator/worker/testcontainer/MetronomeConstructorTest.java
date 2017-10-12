/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.worker.metronome.BusySpinningMetronome;
import com.hazelcast.simulator.worker.metronome.EmptyMetronome;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.SleepingMetronome;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class MetronomeConstructorTest {

    @Test
    public void test() {
        test(NANOSECONDS.toNanos(1), "1ns");
        test(MICROSECONDS.toNanos(1), "1us");
        test(MILLISECONDS.toNanos(1), "1ms");
        test(SECONDS.toNanos(1), "1s");
        test(MINUTES.toNanos(1), "1m");
        test(HOURS.toNanos(1), "1h");
        test(DAYS.toNanos(1), "1d");
    }

    public void test(long expectedInterval, String actualInterval) {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", actualInterval));
        MetronomeConstructor metronomeConstructor = new MetronomeConstructor("", propertyBinding, 1);

        Metronome m = metronomeConstructor.newInstance();
        assertEquals(SleepingMetronome.class, m.getClass());
        SleepingMetronome metronome = (SleepingMetronome) m;

        assertEquals(expectedInterval, metronome.getIntervalNanos());
    }

    @Test(expected = IllegalTestException.class)
    public void testNotInteger() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "foo"));
        new MetronomeConstructor("", propertyBinding, 1);
    }

    @Test(expected = IllegalTestException.class)
    public void testMissingTimeUnit() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "10"));
        new MetronomeConstructor("", propertyBinding, 1);
    }

    @Test(expected = IllegalTestException.class)
    public void negativeInterval() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "-1ns"));
        new MetronomeConstructor("", propertyBinding, 1);
    }

    @Test
    public void testThreadCount() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "20ns"));
        MetronomeConstructor metronomeConstructor = new MetronomeConstructor("", propertyBinding, 10);

        Metronome m = metronomeConstructor.newInstance();
        assertEquals(SleepingMetronome.class, m.getClass());
        SleepingMetronome metronome = (SleepingMetronome) m;

        assertEquals(200, metronome.getIntervalNanos());
    }

    @Test
    public void withCustomMetronome() {
        PropertyBinding propertyBinding = new PropertyBinding(
                new TestCase("foo")
                        .setProperty("interval", "10ns")
                        .setProperty("metronomeClass", BusySpinningMetronome.class));
        MetronomeConstructor metronomeConstructor = new MetronomeConstructor("", propertyBinding, 1);

        Metronome m = metronomeConstructor.newInstance();
        assertEquals(BusySpinningMetronome.class, m.getClass());
        BusySpinningMetronome metronome = (BusySpinningMetronome) m;

        assertEquals(10, metronome.getIntervalNanos());
    }

    @Test
    public void whenZeroInterval() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo"));
        MetronomeConstructor metronomeConstructor = new MetronomeConstructor("", propertyBinding, 5);

        Metronome m = metronomeConstructor.newInstance();
        assertEquals(EmptyMetronome.class, m.getClass());
    }
}
