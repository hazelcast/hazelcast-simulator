package com.hazelcast.simulator.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for spawning threads. You can use your own threads, but make sure that you detect exceptions thrown
 * and report them to the {@link ExceptionReporter}.
 */
public class ThreadSpawner {

    private final List<Thread> threads = Collections.synchronizedList(new LinkedList<Thread>());
    private final ConcurrentMap<String, AtomicInteger> idMap = new ConcurrentHashMap<String, AtomicInteger>();
    private final String testId;

    /**
     * The id of the test this spawner belongs to. This is needed to correlate an exception to a specific test-case.
     *
     * @param testId is allowed to be null, then no correlation is made.
     */
    public ThreadSpawner(String testId) {
        this.testId = testId;
    }

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

        DefaultThread thread = new DefaultThread(getName(namePrefix), runnable);
        threads.add(thread);
        thread.start();
        return thread;
    }

    public void awaitCompletion() {
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<String> getStackTraces() {
        StringBuilder sb = new StringBuilder();
        List<String> stackTraces = new ArrayList<String>(threads.size());
        for (Thread thread : threads) {
            sb.setLength(0);
            for (StackTraceElement stackTraceElement : thread.getStackTrace()) {
                sb.append(stackTraceElement);
            }
            stackTraces.add(sb.toString());
        }
        return stackTraces;
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

    private class DefaultThread extends Thread {
        public DefaultThread(String name, Runnable task) {
            super(task, name);
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                super.run();
            } catch (Throwable t) {
                ExceptionReporter.report(testId, t);
            }
        }
    }
}
