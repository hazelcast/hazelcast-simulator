package com.hazelcast.heartattack.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.heartattack.Coach;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class TellTrainee implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(TellTrainee.class);

    private transient HazelcastInstance hz;
    private final Callable task;
    private final long timeoutSec;

    public TellTrainee(Callable task) {
        this(task, 60);
    }

    public TellTrainee(Callable task, long timeoutSec) {
        this.task = task;
        this.timeoutSec = timeoutSec;
    }

    @Override
    public Object call() throws Exception {
        try {
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            IExecutorService executorr = coach.getTraineeVmManager().getTraineeExecutor();
            Future future = executorr.submit(task);
            return future.get(timeoutSec, TimeUnit.SECONDS);
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
