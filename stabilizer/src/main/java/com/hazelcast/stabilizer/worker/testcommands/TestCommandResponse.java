package com.hazelcast.stabilizer.worker.testcommands;

import java.io.Serializable;

public class TestCommandResponse implements Serializable{

    public static final long serialVersionUID = 0l;

    public long commandId;
    public Object result;

    @Override
    public String toString() {
        return "TestCommandResponse{" +
                "commandId=" + commandId +
                ", result=" + result +
                '}';
    }
}
