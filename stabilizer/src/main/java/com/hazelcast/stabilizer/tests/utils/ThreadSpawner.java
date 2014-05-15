package com.hazelcast.stabilizer.tests.utils;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadSpawner {

    private final List<Thread> threads = Collections.synchronizedList(new LinkedList<Thread>());
    private final ConcurrentMap<String, AtomicInteger> idMap = new ConcurrentHashMap<String, AtomicInteger>();

    public Thread spawn(Runnable runnable) {
        return spawn("Thread", runnable);
    }

    public Thread spawn(final String namePrefix, final Runnable runnable) {
        if (namePrefix == null) {
            throw new NullPointerException("namePrefix can't be null");
        }

        if (runnable == null) {
            throw new NullPointerException("runnable can't be null");
        }

        DefaultThread t = new DefaultThread(getName(namePrefix), runnable);
        threads.add(t);
        t.start();
        return t;
    }

    private String getName(String prefix) {
        AtomicInteger idGenerator = idMap.get(prefix);
        if (idGenerator == null) {
            idGenerator = new AtomicInteger();
            AtomicInteger result = idMap.putIfAbsent(prefix, idGenerator);
            idGenerator = result == null ? idGenerator : result;
        }

        return prefix + "-" + idGenerator.incrementAndGet();
    }

    public void awaitCompletion() {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class DefaultThread extends Thread {
        public DefaultThread(String name, Runnable task) {
            super(task, name);
            setDaemon(true);
        }

        public void run() {
            try {
                super.run();
            } catch (Throwable t) {
                ExceptionReporter.report(t);
            }
        }
    }
}
