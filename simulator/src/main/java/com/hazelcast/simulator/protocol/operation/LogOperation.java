package com.hazelcast.simulator.protocol.operation;

import org.apache.log4j.Level;

public class LogOperation implements SimulatorOperation {

    private final String message;
    private final String level;

    public LogOperation(String message) {
        this(message, Level.INFO);
    }

    public LogOperation(String message, Level level) {
        this.message = message;
        this.level = level.toString();
    }

    public String getMessage() {
        return message;
    }

    public Level getLevel() {
        return Level.toLevel(level, Level.INFO);
    }
}
