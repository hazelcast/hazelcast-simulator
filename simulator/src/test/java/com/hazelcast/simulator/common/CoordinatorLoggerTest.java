package com.hazelcast.simulator.common;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CoordinatorLoggerTest {

    private ServerConnector serverConnector;
    private CoordinatorLogger coordinatorLogger;

    @Before
    public void setUp() {
        serverConnector = mock(ServerConnector.class);
        coordinatorLogger = new CoordinatorLogger(serverConnector);
    }

    @Test
    public void testTrace() {
        coordinatorLogger.trace("trace message");

        verifyServerConnector();
    }

    @Test
    public void testDebug() {
        coordinatorLogger.debug("debug message");

        verifyServerConnector();
    }

    @Test
    public void testInfo() {
        coordinatorLogger.info("info message");

        verifyServerConnector();
    }

    @Test
    public void testWarn() {
        coordinatorLogger.warn("warn message");

        verifyServerConnector();
    }

    @Test
    public void testError() {
        coordinatorLogger.error("error message");

        verifyServerConnector();
    }

    @Test
    public void testFatal() {
        coordinatorLogger.fatal("fatal message");

        verifyServerConnector();
    }

    private void verifyServerConnector() {
        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }
}
