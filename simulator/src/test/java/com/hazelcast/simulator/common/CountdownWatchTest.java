package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CountdownWatchTest {

    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateNegativeDelay() {
        CountdownWatch.started(Long.MIN_VALUE);
    }

    @Test
    public void overflowProtection() {
        CountdownWatch watch = CountdownWatch.started(Long.MAX_VALUE);
        assertFalse(watch.isDone());
    }

    @Test
    public void unbounded() {
        CountdownWatch watch = CountdownWatch.unboundedStarted();
        assertFalse(watch.isDone());
        assertThat(watch.getRemainingMs(), is(Long.MAX_VALUE));
    }

    @Test
    public void bounded() throws Exception {
        long delay = TimeUnit.SECONDS.toMillis(1);
        long startTime = System.currentTimeMillis();
        CountdownWatch watch = CountdownWatch.started(delay);

        assertTrue(!watch.isDone() || System.currentTimeMillis() > startTime + delay);

        Thread.sleep(delay / 2);
        long middle = watch.getRemainingMs();
        assertTrue(middle < delay);
        assertTrue(!watch.isDone() || System.currentTimeMillis() > startTime + delay);

        Thread.sleep(delay / 2 + 1);
        assertTrue(watch.isDone());
        assertThat(watch.getRemainingMs(), is(0l));
        assertTrue(middle == 0 || middle > watch.getRemainingMs());
    }

    @Test
    public void nonNegativeRemainingWhenDone() throws Exception {
        CountdownWatch watch = CountdownWatch.started(1);
        Thread.sleep(10);
        assertThat(watch.getRemainingMs(), is(0l));
    }

}
