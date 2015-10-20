package com.hazelcast.simulator.common;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import org.apache.log4j.Level;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

/**
 * Logs messages to a {@link org.apache.log4j.Logger} instance on the Coordinator.
 */
public class CoordinatorLogger {

    private final ServerConnector serverConnector;

    public CoordinatorLogger(ServerConnector serverConnector) {
        this.serverConnector = serverConnector;
    }

    /**
     * Logs a message object to the Coordinator log with the {@link Level#TRACE} Level.
     *
     * @param message the message object to log
     */
    public void trace(String message) {
        log(message, Level.TRACE);
    }

    /**
     * Logs a message object to the Coordinator log with the {@link Level#DEBUG} Level.
     *
     * @param message the message object to log
     */
    public void debug(String message) {
        log(message, Level.DEBUG);
    }

    /**
     * Logs a message object to the Coordinator log with the {@link Level#INFO} Level.
     *
     * @param message the message object to log
     */
    public void info(String message) {
        log(message, Level.INFO);
    }

    /**
     * Logs a message object to the Coordinator log with the {@link Level#WARN} Level.
     *
     * @param message the message object to log
     */
    public void warn(String message) {
        log(message, Level.WARN);
    }

    /**
     * Logs a message object to the Coordinator log with the {@link Level#ERROR} Level.
     *
     * @param message the message object to log
     */
    public void error(String message) {
        log(message, Level.ERROR);
    }

    /**
     * Logs a message object to the Coordinator log with the {@link Level#FATAL} Level.
     *
     * @param message the message object to log
     */
    public void fatal(String message) {
        log(message, Level.FATAL);
    }

    private void log(String message, Level level) {
        LogOperation operation = new LogOperation(message, level);
        serverConnector.submit(COORDINATOR, operation);
    }
}
