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

    ServerConnector serverConnector;
    CoordinatorLogger coordinatorLogger;

    @Before
    public void setUp() {
        serverConnector = mock(ServerConnector.class);
        coordinatorLogger = new CoordinatorLogger(serverConnector);
    }

    @Test
    public void testTrace() {
        coordinatorLogger.trace("trace message");

        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void testDebug() {
        coordinatorLogger.debug("debug message");

        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void testInfo() {
        coordinatorLogger.info("info message");

        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void testWarn() {
        coordinatorLogger.warn("warn message");

        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void testError() {
        coordinatorLogger.error("error message");

        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }

    @Test
    public void testFatal() {
        coordinatorLogger.fatal("fatal message");

        verify(serverConnector).submit(eq(SimulatorAddress.COORDINATOR), any(LogOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }
}
