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

public class ShoutToTraineesTask implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(ShoutToTraineesTask.class);

    private final Callable task;
    private final String taskDescription;
    private transient HazelcastInstance hz;

    public ShoutToTraineesTask(Callable task, String taskDescription) {
        this.task = task;
        this.taskDescription = taskDescription;
    }

    @Override
    public Object call() throws Exception {
        try {
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            coach.shoutToTrainees(task, taskDescription);
        } catch (Exception e) {
            log.log(Level.SEVERE, format("Failed to execute [%s]", taskDescription), e);
            throw e;
        }

        return null;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }
}

