/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.utils;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

/**
 * Responsible for spawning and waiting for threads.
 *
 * If used in a test context {@link #testId} should be set to the testId of that test. This is needed to correlate an
 * exception to a specific test case. In a test context you should not set {@link #throwException} to <code>true</code>,
 * so the {@link ExceptionReporter} will be used.
 *
 * You can also use your own threads in Simulator tests, but make sure that you detect thrown exceptions and report them to the
 * {@link ExceptionReporter} by yourself.
 */
public class ThreadSpawner {

    private final List<Thread> threads = Collections.synchronizedList(new LinkedList<>());
    private final ConcurrentMap<String, AtomicInteger> idMap = new ConcurrentHashMap<>();

    private final String testId;
    private final boolean throwException;
    private final UncaughtExceptionHandler exceptionHandler;

    private volatile Throwable caughtException;

    /**
     * Creates a default {@link ThreadSpawner} for a test context.
     *
     * All occurring exceptions will be reported to the {@link ExceptionReporter}.
     *
     * @param testId testId to give reported exceptions a context
     */
    public ThreadSpawner(String testId) {
        this(testId, false);
    }

    /**
     * Creates a {@link ThreadSpawner} which can report exceptions to the {@link ExceptionReporter} or throw them directly.
     *
     * @param testId     testId to give reported exceptions a context
     * @param throwException <code>true</code> if exceptions should be directly thrown,
     *                       <code>false</code> if {@link ExceptionReporter} should be used
     */
    public ThreadSpawner(String testId, boolean throwException) {
        this.testId = testId;
        this.throwException = throwException;
        this.exceptionHandler = initExceptionHandler(throwException);
    }

    private UncaughtExceptionHandler initExceptionHandler(boolean throwException) {
        if (!throwException) {
            return null;
        }
        return (th, ex) -> {
            if (caughtException == null) {
                caughtException = ex;
            }
        };
    }

    /**
     * Spawns a new thread for the given {@link Runnable}.
     *
     * @param runnable the {@link Runnable} to execute
     * @return the created thread
     */
    public Thread spawn(Runnable runnable) {
        return spawn(testId + "-Thread", runnable);
    }

    /**
     * Spawns a new thread for the given {@link Runnable}.
     *
     * @param namePrefix the name prefix for the thread
     * @param runnable   the {@link Runnable} to execute
     * @return the created thread
     */
    public Thread spawn(String namePrefix, Runnable runnable) {
        checkNotNull(namePrefix, "namePrefix can't be null");
        checkNotNull(runnable, "runnable can't be null");

        String name = newName(namePrefix);
        Thread thread;
        if (throwException) {
            thread = new ThrowExceptionThread(name, runnable);
            thread.setUncaughtExceptionHandler(exceptionHandler);
        } else {
            thread = new ReportExceptionThread(testId, name, runnable);
        }
        threads.add(thread);
        thread.start();
        return thread;
    }

    /**
     * Waits for all threads to finish.
     *
     * If {@link #throwException} is <code>true</code> this method will throw the first occurred exception of a thread.
     */
    public void awaitCompletion() {
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw rethrow(e);
            }
        }
        if (caughtException != null) {
            throw rethrow(caughtException);
        }
    }

    /**
     * Interrupts all running threads.
     */
    public void interruptAll() {
        for (Thread thread : threads) {
            thread.interrupt();
        }
    }

    private String newName(String prefix) {
        AtomicInteger idGenerator = idMap.get(prefix);
        if (idGenerator == null) {
            idGenerator = new AtomicInteger();
            AtomicInteger result = idMap.putIfAbsent(prefix, idGenerator);
            idGenerator = result == null ? idGenerator : result;
        }

        return prefix + '-' + idGenerator.incrementAndGet();
    }

    private static class ThrowExceptionThread extends Thread {

        ThrowExceptionThread(String name, Runnable task) {
            super(task, name);
            setDaemon(true);
        }
    }

    private static class ReportExceptionThread extends Thread {

        private final String testId;

        ReportExceptionThread(String testId, String name, Runnable task) {
            super(task, name);
            this.testId = testId;
            setDaemon(true);
        }

        @Override
        @SuppressWarnings("PMD.AvoidCatchingThrowable")
        public void run() {
            try {
                super.run();
            } catch (Throwable t) {
                ExceptionReporter.report(testId, t);
            }
        }
    }
}
