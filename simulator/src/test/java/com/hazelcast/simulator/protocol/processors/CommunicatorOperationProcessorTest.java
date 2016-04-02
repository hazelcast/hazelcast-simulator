package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.junit.Assert.assertEquals;

public class CommunicatorOperationProcessorTest {

    private CommunicatorOperationProcessor processor;

    @BeforeClass
    public static void setUpEnvironment() {
        setLogLevel(Level.TRACE);
    }

    @AfterClass
    public static void resetEnvironment() {
        resetLogLevel();
    }

    @Before
    public void setUp() {
        LocalExceptionLogger exceptionLogger = new LocalExceptionLogger();

        processor = new CommunicatorOperationProcessor(exceptionLogger);
    }

    @Test
    public void testProcessOperation_withIntegrationTestOperation() {
        SimulatorOperation operation = new IntegrationTestOperation();
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(SUCCESS, responseType);
    }

    @Test
    public void testProcessOperation_withIntegrationTestOperation_withException() {
        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.Type.EQUALS, "foo");
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
    }

    @Test
    public void testProcessOperation_withIntegrationTestOperation_withUnsupportedOperation() {
        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.Type.DEEP_NESTED_ASYNC);
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void testProcessOperation_unsupportedOperation() {
        SimulatorOperation operation = new StopTestOperation();
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }
}
