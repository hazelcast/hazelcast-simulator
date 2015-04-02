package com.hazelcast.simulator.utils.helper;

@SuppressWarnings("unused")
public final class ExitException extends SecurityException {

    private final int status;

    public ExitException(int status) {
        super();
        this.status = status;
    }

    public int getStatus() {
        return status;
    }
}
