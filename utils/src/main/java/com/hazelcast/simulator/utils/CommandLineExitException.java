package com.hazelcast.simulator.utils;

/**
 * This exception is thrown when the process should exit with status code 1.
 */
public class CommandLineExitException extends RuntimeException {

    public CommandLineExitException(String message) {
        super(message);
    }

    public CommandLineExitException(String message, Throwable cause) {
        super(message, cause);
    }
}
