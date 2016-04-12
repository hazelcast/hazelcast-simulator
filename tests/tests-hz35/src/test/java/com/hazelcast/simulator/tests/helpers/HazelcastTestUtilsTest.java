package com.hazelcast.simulator.tests.helpers;

import org.junit.Test;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class HazelcastTestUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(HazelcastTestUtils.class);
    }

    @Test
    public void testRethrow_RuntimeException() {
        Throwable throwable = new RuntimeException();
        try {
            throw rethrow(throwable);
        } catch (RuntimeException e) {
            assertEquals(throwable, e);
        }
    }

    @Test
    public void testRethrow_Throwable() {
        Throwable throwable = new Throwable();
        try {
            throw rethrow(throwable);
        } catch (RuntimeException e) {
            assertEquals(throwable, e.getCause());
        }
    }
}
