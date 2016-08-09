package com.hazelcast.simulator;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSupport {

    public static <E> Future<E> spawn(Callable<E> e) {
        FutureTask<E> task = new FutureTask<E>(e);
        Thread thread = new Thread(task);
        thread.start();
        return task;
    }

    @SuppressWarnings("unchecked")
    public static <E> E assertInstanceOf(Class<E> clazz, Object object) {
        assertNotNull(object);
        assertTrue(object + " is not an instanceof " + clazz.getName(), clazz.isAssignableFrom(object.getClass()));
        return (E) object;
    }
}
