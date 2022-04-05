package com.hazelcast.simulator.test;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BaseThreadStateTest {

    private BaseThreadState state;

    @Before
    public void setup() {
        state = new BaseThreadState();
    }

    @Test
    public void randomInt_withBound() {
        int value = state.randomInt(100);
        assertTrue(value >= 0);
        assertTrue(value < 100);
    }

    @Test
    public void randomLong_withBound() {
        long value = state.randomLong(100);
        assertTrue(value >= 0);
        assertTrue(value < 100);
    }

    @Test(expected = IllegalArgumentException.class)
    public void randomLong_withNegativeBound() {
        state.randomLong(-1);
    }

    @Test
    public void randomBoolean() {
        boolean whatever = state.randomBoolean();
    }

    @Test
    public void randomInteger() {
        int whatever = state.randomInt();
    }

    @Test
    public void randomDouble() {
        double whatever = state.randomDouble();
    }

    @Test
    public void randomLong() {
        double whatever = state.randomLong();
    }
}
