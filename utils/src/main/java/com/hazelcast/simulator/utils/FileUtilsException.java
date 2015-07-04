package com.hazelcast.simulator.utils;

public class FileUtilsException extends RuntimeException {

    public FileUtilsException(String message) {
        super(message);
    }

    public FileUtilsException(String message, Throwable cause) {
        super(message, cause);
    }

    public FileUtilsException(Throwable cause) {
        super(cause);
    }
}
