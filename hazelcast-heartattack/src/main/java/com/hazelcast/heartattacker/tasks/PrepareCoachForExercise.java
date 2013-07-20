package com.hazelcast.heartattacker.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattacker.Coach;
import com.hazelcast.heartattacker.ExerciseRecipe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class PrepareCoachForExercise implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(PrepareCoachForExercise.class);

    private transient HazelcastInstance hz;
    private final ExerciseRecipe exerciseRecipe;

    public PrepareCoachForExercise(ExerciseRecipe exerciseRecipe) {
        this.exerciseRecipe = exerciseRecipe;
    }

    @Override
    public Object call() throws Exception {
        log.log(Level.INFO, "Preparing coach for exercise");

        try {
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            coach.setExerciseRecipe(exerciseRecipe);
            return null;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to init coach Exercise", e);
            throw e;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}
