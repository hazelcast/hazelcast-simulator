package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.join;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CommonUtils_MiscTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CommonUtils.class);
    }

    @Test
    public void testJoin() throws Exception {
        Iterable<String> input = Arrays.asList("one", "two", "three");
        String joined = join(input);
        assertEquals("one, two, three", joined);
    }

    @Test
    public void testJoinEmptyString() throws Exception {
        String joined = join(Collections.EMPTY_LIST);
        assertEquals("", joined);
    }

    @Test
    public void testGetSimulatorVersion() {
        assertNull(getSimulatorVersion());
    }

    @Test
    public void testFixRemoteStackTrace() {
        Throwable remoteCause = new Throwable("Expected throwable");
        StackTraceElement[] localSideStackTrace = Thread.currentThread().getStackTrace();
        int expectedLength = remoteCause.getStackTrace().length + localSideStackTrace.length;

        fixRemoteStackTrace(remoteCause, localSideStackTrace);

        assertEquals("Expected stack trace of length %d, but was %d", expectedLength, remoteCause.getStackTrace().length);
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

    @Test
    public void testThrowableToString() {
        String marker = "#*+*#";
        Throwable throwable = new Throwable(marker);
        String actual = throwableToString(throwable);

        assertTrue(format("Expected throwable string to contain marker %s, but was %s", marker, actual), actual.contains(marker));
    }
}
