package com.hazelcast.simulator.test;

public enum TestPhase {
    SETUP("setup"),
    LOCAL_WARMUP("local warmup"),
    GLOBAL_WARMUP("global warmup"),
    RUN("run"),
    GLOBAL_VERIFY("global verify"),
    LOCAL_VERIFY("local verify"),
    GLOBAL_TEARDOWN("global tear down"),
    LOCAL_TEARDOWN("local tear down");

    public final String name;

    TestPhase(String name) {
        this.name = name;
    }
}
