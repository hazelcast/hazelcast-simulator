package com.hazelcast.simulator.tests.webContainer;

/*
* common methods to manipulate a ServletContainer
* */
public interface ServletContainer {

    void restart() throws Exception;

    void stop() throws Exception;

    void start() throws Exception;

    boolean isRunning();

}
