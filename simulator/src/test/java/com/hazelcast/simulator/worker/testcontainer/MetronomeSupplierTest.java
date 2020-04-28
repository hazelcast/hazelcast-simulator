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

public class MetronomeSupplierTest {

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
        MetronomeSupplier supplier = new MetronomeSupplier("", propertyBinding, 1);

        Metronome m = supplier.get();
        assertEquals(SleepingMetronome.class, m.getClass());
        SleepingMetronome metronome = (SleepingMetronome) m;

        assertEquals(expectedInterval, metronome.getIntervalNanos());
    }

    @Test(expected = IllegalTestException.class)
    public void testNotInteger() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "foo"));
        new MetronomeSupplier("", propertyBinding, 1);
    }

    @Test(expected = IllegalTestException.class)
    public void testMissingTimeUnit() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "10"));
        new MetronomeSupplier("", propertyBinding, 1);
    }

    @Test(expected = IllegalTestException.class)
    public void negativeInterval() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "-1ns"));
        new MetronomeSupplier("", propertyBinding, 1);
    }

    @Test
    public void testThreadCount() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo").setProperty("interval", "20ns"));
        MetronomeSupplier supplier = new MetronomeSupplier("", propertyBinding, 10);

        Metronome m = supplier.get();
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
        MetronomeSupplier supplier = new MetronomeSupplier("", propertyBinding, 1);

        Metronome m = supplier.get();
        assertEquals(BusySpinningMetronome.class, m.getClass());
        BusySpinningMetronome metronome = (BusySpinningMetronome) m;

        assertEquals(10, metronome.getIntervalNanos());
    }

    @Test
    public void whenZeroInterval() {
        PropertyBinding propertyBinding = new PropertyBinding(new TestCase("foo"));
        MetronomeSupplier supplier = new MetronomeSupplier("", propertyBinding, 5);

        Metronome m = supplier.get();
        assertEquals(EmptyMetronome.class, m.getClass());
    }
}
