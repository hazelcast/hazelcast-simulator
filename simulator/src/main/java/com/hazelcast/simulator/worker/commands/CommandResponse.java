package com.hazelcast.simulator.worker.commands;

import java.io.Serializable;

public class CommandResponse implements Serializable {

    public static final long serialVersionUID = 0l;

    public long commandId;
    public Object result;

    @Override
    public String toString() {
        return "CommandResponse{" +
                "commandId=" + commandId +
                ", result=" + result +
                '}';
    }
}
