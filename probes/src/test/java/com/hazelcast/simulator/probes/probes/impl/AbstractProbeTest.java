package com.hazelcast.simulator.probes.probes.impl;

import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;

public abstract class AbstractProbeTest {

    static void sleepMillis(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    static void sleepNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }

        LockSupport.parkNanos(nanos);
    }

    @SuppressWarnings("unchecked")
    static <E> E getObjectFromField(Object object, String fieldName) {
        if (object == null) {
            throw new NullPointerException("Object to retrieve field from can't be null");
        }

        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E) field.get(object);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    static void assertEqualsStringFormat(String message, Object expected, Object actual) {
        assertEquals(String.format(message, expected, actual), expected, actual);
    }

    static void assertEqualsStringFormat(String message, Double expected, Double actual, Double delta) {
        assertEquals(String.format(message, expected, actual), expected, actual, delta);
    }
}
