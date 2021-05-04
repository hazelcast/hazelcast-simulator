package com.hazelcast.simulator.tests.platform.nexmark.model;

public class Event {

    private final long id;
    private final long timestamp;

    public Event(long id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public long id() {
        return id;
    }

    public long timestamp() {
        return timestamp;
    }
}
