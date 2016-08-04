package com.hazelcast.simulator.worker.metronome;

import org.junit.Test;

import static com.hazelcast.simulator.TestSupport.assertInstanceOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static junit.framework.TestCase.assertEquals;

public class MetronomeBuilderTest {

    @Test
    public void withRatePerSecond() {
        // 100 per second is once every 10 ms
        MetronomeBuilder builder = new MetronomeBuilder()
                .withRatePerSecond(100);

        assertEquals(MILLISECONDS.toNanos(10), builder.getIntervalNanos());
    }

    @Test
    public void whenZeroInterval() {
        MetronomeBuilder builder = new MetronomeBuilder()
                .withIntervalMicros(0);

        assertInstanceOf(EmptyMetronome.class, builder.build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNegativeIntervalMicros() {
        new MetronomeBuilder()
                .withIntervalMicros(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenNegativeIntervalMillis() {
        new MetronomeBuilder()
                .withIntervalMillis(-1);
    }
}
