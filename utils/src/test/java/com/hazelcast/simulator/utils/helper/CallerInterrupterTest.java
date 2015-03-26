package com.hazelcast.simulator.utils.helper;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSecondsThrowException;

public class CallerInterrupterTest {

    @Test(expected = RuntimeException.class)
    public void testCallerInterrupter() {
        new CallerInterrupter(Thread.currentThread(), TimeUnit.SECONDS.toNanos(1)).start();

        sleepSecondsThrowException(Integer.MAX_VALUE);
    }
}
