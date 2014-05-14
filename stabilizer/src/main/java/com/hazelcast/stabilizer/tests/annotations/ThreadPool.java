package com.hazelcast.stabilizer.tests.annotations;

import com.hazelcast.stabilizer.worker.ExceptionReporter;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ThreadPool {

    private final List<Thread> threads = Collections.synchronizedList(new LinkedList<Thread>());

    public Thread spawn(Runnable runnable){
        Thread t = new Thread(new CatchingRunnable(runnable));
        threads.add(t);
        t.start();
        return t;
    }

    public void awaitCompletion() {
        for(Thread t: threads){
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class CatchingRunnable implements Runnable {
        private final Runnable runnable;

        private CatchingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                ExceptionReporter.report(t);
            }
        }
    }
}
