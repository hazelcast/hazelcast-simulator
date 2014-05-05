package com.hazelcast.stabilizer.agent;

public class SpawnWorkerFailedException extends RuntimeException{
    public SpawnWorkerFailedException() {
    }

    public SpawnWorkerFailedException(String message) {
        super(message);
    }

    public SpawnWorkerFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public SpawnWorkerFailedException(Throwable cause) {
        super(cause);
    }

    public SpawnWorkerFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
