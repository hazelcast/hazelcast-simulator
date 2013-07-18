package com.hazelcast.heartattack.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattack.Coach;
import com.hazelcast.heartattack.TraineeVmSettings;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import static java.lang.String.format;

public class SpawnTrainees implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(SpawnTrainees.class);

    private transient HazelcastInstance hz;
    private final TraineeVmSettings settings;

    public SpawnTrainees(TraineeVmSettings settings) {
        this.settings = settings;
    }

    @Override
    public Object call() throws Exception {
        log.log(Level.INFO, format("Spawning %s trainees", settings.getTraineeCount()));

        try {
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            coach.getTraineeVmManager().spawn(settings);
            return null;
        } catch (Exception e) {
            log.log(Level.SEVERE, "Failed to spawn Trainee Virtual Machines", e);
            throw e;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}
