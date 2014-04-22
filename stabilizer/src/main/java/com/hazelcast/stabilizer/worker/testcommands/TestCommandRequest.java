package com.hazelcast.stabilizer.worker.testcommands;

import java.io.Serializable;

public class TestCommandRequest implements Serializable {

    public static final long serialVersionUID = 0l;

    public long id;
    public TestCommand task;

    @Override
    public String toString() {
        return "TestCommandRequest{" +
                "id=" + id +
                ", task=" + task +
                '}';
    }
}
