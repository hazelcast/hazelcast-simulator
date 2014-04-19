package com.hazelcast.stabilizer.worker.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.Test;

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
            log.info("Calling test.stop");

            Test test = (Test) hz.getUserContext().get(Test.TEST_INSTANCE);
            if (test == null) {
                throw new IllegalStateException("No test to stop");
            }
            test.stop(timeoutMs);
            log.info("Finished calling test.stop()");
            return null;
        } catch (Exception e) {
            log.severe("Failed to execute test.stop", e);
            throw e;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}