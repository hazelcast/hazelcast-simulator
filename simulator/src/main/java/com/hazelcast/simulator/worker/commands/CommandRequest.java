package com.hazelcast.simulator.worker.commands;

import java.io.Serializable;

public class CommandRequest implements Serializable {

    public static final long serialVersionUID = 0l;

    public long id;
    public Command task;

    @Override
    public String toString() {
        return "CommandRequest{"
                + "id=" + id
                + ", task=" + task
                + '}';
    }
}
