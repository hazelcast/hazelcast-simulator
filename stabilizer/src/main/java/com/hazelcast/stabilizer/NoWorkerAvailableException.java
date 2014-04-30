package com.hazelcast.stabilizer;

public class NoWorkerAvailableException extends RuntimeException {
    public NoWorkerAvailableException(String msg){
        super(msg);
    }
}
