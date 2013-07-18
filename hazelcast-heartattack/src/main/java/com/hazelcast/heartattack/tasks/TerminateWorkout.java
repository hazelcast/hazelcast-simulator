package com.hazelcast.heartattack.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattack.Coach;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import static java.lang.String.format;

public class TerminateWorkout implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(TerminateWorkout.class);

    private transient HazelcastInstance hz;

    @Override
    public Object call() throws Exception {
        log.log(Level.INFO, "TerminateWorkout");

        long startMs = System.currentTimeMillis();

        Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
        coach.terminateWorkout();

        long durationMs = System.currentTimeMillis() - startMs;
        log.log(Level.INFO, format("TerminateWorkout finished in %s ms", durationMs));
        return null;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }
}
