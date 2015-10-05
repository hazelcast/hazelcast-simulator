package com.hazelcast.simulator.worker.performance;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class PerformanceUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(PerformanceUtils.class);
    }

    @Test
    public void testGetNumberOfDigits() {
        assertEquals(1, PerformanceUtils.getNumberOfDigits(1));
        assertEquals(1, PerformanceUtils.getNumberOfDigits(5));
        assertEquals(1, PerformanceUtils.getNumberOfDigits(9));

        assertEquals(2, PerformanceUtils.getNumberOfDigits(10));
        assertEquals(2, PerformanceUtils.getNumberOfDigits(55));
        assertEquals(2, PerformanceUtils.getNumberOfDigits(99));

        assertEquals(3, PerformanceUtils.getNumberOfDigits(100));
        assertEquals(3, PerformanceUtils.getNumberOfDigits(500));
    }
}
