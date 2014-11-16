package com.hazelcast.stabilizer.worker.commands;

import java.io.Serializable;

public abstract class Command implements Serializable {

    public static final long serialVersionUID = 0l;

    public transient long enteredMs = System.currentTimeMillis();

    public boolean awaitReply() {
        return true;
    }
}
