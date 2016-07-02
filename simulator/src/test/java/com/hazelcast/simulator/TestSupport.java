package com.hazelcast.simulator;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class TestSupport {

    public static void deleteGeneratedRunners() {
        for (File file : new File(System.getProperty("user.dir")).listFiles()) {
            String name = file.getName();
            if (name.endsWith(".java") || name.endsWith(".class")) {
                if (name.contains("Runner")) {
                    file.delete();
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
