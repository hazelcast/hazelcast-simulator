package com.hazelcast.simulator;

import java.util.HashMap;
import java.util.Map;
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

    public static Map<String, String> toMap(String... array) {
        if (array.length % 2 == 1) {
            throw new IllegalArgumentException("even number of arguments expected");
        }

        Map<String, String> result = new HashMap<String, String>();
        for (int k = 0; k < array.length; k += 2) {
            result.put(array[k], array[k + 1]);
        }
        return result;
    }


}
