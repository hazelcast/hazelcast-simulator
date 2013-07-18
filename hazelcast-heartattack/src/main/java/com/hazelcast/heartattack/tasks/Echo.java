package com.hazelcast.heartattack.tasks;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class Echo implements Callable, Serializable {
    private final static ILogger log = Logger.getLogger(Echo.class);

    private final String msg;

    public Echo(String msg) {
        this.msg = msg;
    }

    @Override
    public Object call() throws Exception {
        log.log(Level.INFO, msg);
        return null;
    }

}
