package com.hazelcast.simulator;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Created by alarmnummer on 7/9/16.
 */
public class TestSupport {

    public static <E> Future<E> spawn(Callable<E> e){
        FutureTask<E> task = new FutureTask<E>(e);
        Thread thread = new Thread(task);
        thread.start();
        return task;
    }
}
