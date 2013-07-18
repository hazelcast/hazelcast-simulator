package com.hazelcast.heartattack.exercises;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.heartattack.Utils;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

public abstract class AbstractExercise implements Exercise {


    final static ILogger log = Logger.getLogger(AbstractExercise.class.getName());

    protected HazelcastInstance hazelcastInstance;

    protected String exerciseId;
    protected volatile boolean stop = false;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final Set<Thread> threads = new HashSet<Thread>();

    public String getExerciseId() {
        return exerciseId;
    }

    public void setExerciseId(String exerciseId) {
        this.exerciseId = exerciseId;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void globalSetup() throws Exception {
    }

    @Override
    public void localSetup() throws Exception {
    }

    @Override
    public void localTearDown() throws Exception {
    }

    @Override
    public void globalTearDown() throws Exception {
    }

    @Override
    public void globalVerify() throws Exception {
    }

    @Override
    public void localVerify() throws Exception {
    }

    public final Thread spawn(Runnable runnable) {
        Thread thread = new Thread(new CatchingRunnable(runnable));
        threads.add(thread);
        thread.start();
        return thread;
    }

    private class CatchingRunnable implements Runnable {
        private final Runnable runnable;

        private CatchingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                startLatch.await();
                runnable.run();
            } catch (Throwable t) {

                log.log(Level.SEVERE, "Error detected", t);
                Utils.sleepSeconds(2);

                ExerciseUtils.signalHeartAttack(t);
            }
        }
    }

    @Override
    public void start() {
        startLatch.countDown();
    }

    @Override
    public void stop() throws InterruptedException {
        stop = true;
        for (Thread thread : threads) {
            thread.join();
        }
        threads.clear();
    }
}
