package com.hazelcast.simulator;

import org.junit.Assert;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static com.hazelcast.simulator.utils.FileUtils.delete;
import static org.junit.Assert.assertTrue;

public class TestSupport {

    public static <E> E assertInstanceOf(Class<E> clazz, Object object) {
        Assert.assertNotNull(object);
        assertTrue(object + " is not an instanceof " + clazz.getName(), clazz.isAssignableFrom(object.getClass()));
        return (E) object;
    }

    public static void deleteGeneratedRunners() {
        File[] files = new File(System.getProperty("user.dir")).listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            String name = file.getName();
            if (name.endsWith(".java") || name.endsWith(".class")) {
                if (name.contains("Runner")) {
                    delete(file);
                }
            }
        }
    }

    public static <E> Future<E> spawn(Callable<E> e) {
        FutureTask<E> task = new FutureTask<E>(e);
        Thread thread = new Thread(task);
        thread.start();
        return task;
    }
}
