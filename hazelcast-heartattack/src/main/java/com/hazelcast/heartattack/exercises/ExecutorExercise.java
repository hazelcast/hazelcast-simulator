package com.hazelcast.heartattack.exercises;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class ExecutorExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(ExecutorExercise.class);

    private IExecutorService[] executors;
    private IAtomicLong executedCounter;
    private IAtomicLong expectedExecutedCounter;

    public int executorCount = 1;

    //the number of threads submitting tasks to the executor.
    public int threadCount = 5;

    //the number of outstanding submits, before doing get. A count of 1 means that you wait for every task
    //to complete, before sending in the next.
    public int submitCount = 5;


    @Override
    public void localSetup() {
        log.log(Level.INFO, "localSetup");
        executors = new IExecutorService[executorCount];
        for (int k = 0; k < executors.length; k++) {
            executors[k] = getHazelcastInstance().getExecutorService(exerciseId + ":Executor-" + k);
        }
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
        executedCounter = hazelcastInstance.getAtomicLong(exerciseId + ":ExecutedCounter");
        expectedExecutedCounter = hazelcastInstance.getAtomicLong(exerciseId + ":ExpectedExecutedCounter");
    }

    @Override
    public void globalTearDown() throws Exception {
        executedCounter.destroy();
        expectedExecutedCounter.destroy();
        for (IExecutorService executor : executors) {
            executor.shutdownNow();
            if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
                log.log(Level.SEVERE, "Time out while waiting for  shutdown of executor: " + executor.getId());
            }
            executor.destroy();
        }
    }

    @Override
    public void globalVerify() throws Exception {
        log.log(Level.INFO, "globalVerify called");
        long actualCount = executedCounter.get();
        long expectedCount = expectedExecutedCounter.get();
        if (actualCount != expectedCount) {
            throw new RuntimeException("ActualCount:" + actualCount + " doesn't match ExpectedCount:" + expectedCount);
        }
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;

            List<Future> futureList = new LinkedList<Future>();
            while (!stop) {
                int index = random.nextInt(executors.length);
                IExecutorService executorService = executors[index];
                futureList.clear();

                for (int k = 0; k < submitCount; k++) {
                    Future future = executorService.submit(new Task());
                    futureList.add(future);
                    iteration++;
                }

                for (Future future : futureList) {
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }

                if (iteration % 10000 == 0) {
                    log.log(Level.INFO, Thread.currentThread().getName() + " At iteration: " + iteration);
                }

            }

            expectedExecutedCounter.addAndGet(iteration);
        }
    }

    private static class Task implements Runnable, Serializable, HazelcastInstanceAware {
        private transient HazelcastInstance hz;

        @Override
        public void run() {
            ExecutorExercise exerciseInstance = (ExecutorExercise) hz.getUserContext().get(EXERCISE_INSTANCE);
            exerciseInstance.executedCounter.incrementAndGet();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }
    }
}
