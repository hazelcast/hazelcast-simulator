package com.hazelcast.stabilizer.worker.testcommands;

import java.io.Serializable;

public class TestCommandResponse implements Serializable{

    public static final long serialVersionUID = 0l;

    public long taskId;
    public Object result;

    @Override
    public String toString() {
        return "TestCommandResponse{" +
                "taskId=" + taskId +
                ", result=" + result +
                '}';
    }
}
