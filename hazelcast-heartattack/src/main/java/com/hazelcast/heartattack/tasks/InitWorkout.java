package com.hazelcast.heartattack.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattack.Coach;
import com.hazelcast.heartattack.Workout;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class InitWorkout implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(InitExercise.class);

    private transient HazelcastInstance hz;
    private final byte[] content;
    private final Workout workout;

    public InitWorkout(Workout workout, byte[] zippedContent) {
        this.content = zippedContent;
        this.workout = workout;
    }

    @Override
    public Object call() throws Exception {
        try {
            log.log(Level.INFO, "Init InitWorkout:"+workout.getId());
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            coach.initWorkout(workout,content);
            return null;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to init InitWorkout", e);
            throw e;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}
