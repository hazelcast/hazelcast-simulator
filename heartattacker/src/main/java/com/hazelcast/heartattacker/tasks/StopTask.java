package com.hazelcast.heartattacker.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattacker.exercises.Exercise;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class StopTask implements Callable, Serializable, HazelcastInstanceAware {

    private final static ILogger log = Logger.getLogger(StopTask.class);

    private transient HazelcastInstance hz;
    private final long timeoutMs;

    public StopTask(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Object call() throws Exception {
        try {
            log.info("Calling exerciseInstance.stop");

            Exercise exercise = (Exercise) hz.getUserContext().get(Exercise.EXERCISE_INSTANCE);
            if (exercise == null) {
                throw new IllegalStateException("No ExerciseInstance to stop");
            }
            exercise.stop(timeoutMs);
            log.info("Finished calling exerciseInstance.stop()");
            return null;
        } catch (Exception e) {
            log.severe("Failed to execute exercise.stop", e);
            throw e;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}