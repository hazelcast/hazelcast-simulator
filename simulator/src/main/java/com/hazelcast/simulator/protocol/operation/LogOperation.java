package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Level;

public class LogOperation implements SimulatorOperation {

    private final SimulatorAddress source;
    private final String message;
    private final String level;

    public LogOperation(SimulatorAddress source, String message) {
        this(source, message, Level.INFO);
    }

    public LogOperation(SimulatorAddress source, String message, Level level) {
        this.source = source;
        this.message = message;
        this.level = level.toString();
    }

    public SimulatorAddress getSource() {
        return source;
    }

    public String getMessage() {
        return message;
    }

    public Level getLevel() {
        return Level.toLevel(level, Level.INFO);
    }
}
