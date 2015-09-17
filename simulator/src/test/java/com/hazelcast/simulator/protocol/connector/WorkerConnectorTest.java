package com.hazelcast.simulator.protocol.connector;

import org.junit.Test;

public class WorkerConnectorTest {

    private static final int WORKER_INDEX = 1;
    private static final int AGENT_INDEX = 1;
    private static final int PORT = 11111;

    @Test
    public void testCreateInstance_withFileExceptionLogger_implicit() {
        WorkerConnector.createInstance(WORKER_INDEX, AGENT_INDEX, PORT);
    }

    @Test
    public void testCreateInstance_withRemoteExceptionLogger_explicit() {
        WorkerConnector.createInstance(WORKER_INDEX, AGENT_INDEX, PORT, false);
    }

    @Test
    public void testCreateInstance_withRemoteExceptionLogger() {
        WorkerConnector.createInstance(WORKER_INDEX, AGENT_INDEX, PORT, true);
    }
}
