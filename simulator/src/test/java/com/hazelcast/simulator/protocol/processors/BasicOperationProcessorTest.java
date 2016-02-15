package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.operation.ChaosMonkeyOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.EQUALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class BasicOperationProcessorTest {

    private LocalExceptionLogger exceptionLogger;
    private IntegrationTestOperationProcessor processor;

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
        exceptionLogger = new LocalExceptionLogger();

        processor = new IntegrationTestOperationProcessor();
    }

    @Test
    public void testProcessIntegrationTestOperation() throws Exception {
        IntegrationTestOperation operation = new IntegrationTestOperation();

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
        assertEquals(0, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testProcessIntegrationTestOperation_withInvalidData() throws Exception {
        IntegrationTestOperation operation = new IntegrationTestOperation(EQUALS, "invalid");

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        assertEquals(1, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testProcessLogOperation() {
        LogOperation operation = new LogOperation("BasicOperationProcessorTest");

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
        assertEquals(0, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testChaosMonkey() {
        ChaosMonkeyOperation operation = new ChaosMonkeyOperation(ChaosMonkeyOperation.Type.INTEGRATION_TEST);

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
        assertEquals(0, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testOtherOperation() {
        TerminateWorkerOperation operation = new TerminateWorkerOperation(0, false);

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertNull(responseType);
        assertEquals(processor.operationType, OperationType.TERMINATE_WORKER);
    }

    private final class IntegrationTestOperationProcessor extends OperationProcessor {

        private OperationType operationType;

        IntegrationTestOperationProcessor() {
            super(BasicOperationProcessorTest.this.exceptionLogger);
        }

        @Override
        protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                                SimulatorAddress sourceAddress) throws Exception {
            this.operationType = operationType;
            return null;
        }
    }
}
