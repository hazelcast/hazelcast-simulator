package com.hazelcast.simulator.protocol.exception;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocalExceptionLoggerTest {

    private LocalExceptionLogger exceptionLogger;

    @Before
    public void setUp() {
        exceptionLogger = new LocalExceptionLogger(SimulatorAddress.COORDINATOR, ExceptionType.COORDINATOR_EXCEPTION);
    }

    @Test
    public void testLog() {
        exceptionLogger.log(new RuntimeException("expected exception"));

        assertEquals(1, exceptionLogger.getExceptionCount());
    }
}
