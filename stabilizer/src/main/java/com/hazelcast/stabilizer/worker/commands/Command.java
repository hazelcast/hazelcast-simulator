package com.hazelcast.stabilizer.worker.commands;

import java.io.Serializable;

public abstract class Command implements Serializable {

    public static final long serialVersionUID = 0l;

    public boolean ignoreTimeout(){
        return false;
    }

    public boolean awaitReply() {
        return true;
    }
}
