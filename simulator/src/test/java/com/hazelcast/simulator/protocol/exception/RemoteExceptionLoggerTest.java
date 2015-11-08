package com.hazelcast.simulator.protocol.exception;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.AssertTask;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.exception.ExceptionLogger.MAX_EXCEPTION_COUNT;
import static com.hazelcast.simulator.protocol.exception.ExceptionType.WORKER_EXCEPTION;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RemoteExceptionLoggerTest {

    private static final long ASSERT_EVENTUALLY_TIMEOUT_SECONDS = 3;

    private ServerConnector serverConnector;
    private RemoteExceptionLogger exceptionLogger;

    @Before
    public void setUp() {
        serverConnector = mock(ServerConnector.class);
        exceptionLogger = new RemoteExceptionLogger(COORDINATOR, WORKER_EXCEPTION, serverConnector);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLog_exceptionIsNull() {
        exceptionLogger.log(null);
    }

    @Test
    public void testLog_exceptionCountExceeded() {
        final int expectedLogInvocationCount = MAX_EXCEPTION_COUNT * 2;
        Exception exception = new IllegalArgumentException("test");

        for (int i = 0; i < expectedLogInvocationCount; i++) {
            exceptionLogger.log(exception);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedLogInvocationCount, exceptionLogger.getLogInvocationCount());
            }
        }, ASSERT_EVENTUALLY_TIMEOUT_SECONDS);

        verify(serverConnector, times(MAX_EXCEPTION_COUNT)).submit(any(SimulatorAddress.class), any(SimulatorOperation.class));
        verifyNoMoreInteractions(serverConnector);
    }
}
