package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.TerminateWorkerOperation;
import com.hazelcast.simulator.worker.Promise;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.EQUALS;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.process;
import static org.junit.Assert.assertEquals;

public class AbstractOperationProcessorTest {

    private IntegrationTestOperationProcessor processor;

    @Before
    public void before() {
        processor = new IntegrationTestOperationProcessor();
    }

    @Test
    public void testProcessIntegrationTestOperation() throws Exception {
        IntegrationTestOperation operation = new IntegrationTestOperation();

        ResponseType responseType = process(processor, operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
        //assertEquals(0, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testProcessIntegrationTestOperation_withInvalidData() throws Exception {
        IntegrationTestOperation operation = new IntegrationTestOperation(EQUALS, "invalid");

        ResponseType responseType = process(processor, operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION, responseType);
        //assertEquals(1, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testProcessLogOperation() throws Exception {
        LogOperation operation = new LogOperation("BasicOperationProcessorTest");

        ResponseType responseType = process(processor, operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
        //assertEquals(0, exceptionLogger.getExceptionCount());
    }

    @Test
    public void testOtherOperation() throws Exception {
        TerminateWorkerOperation operation = new TerminateWorkerOperation(0, false);

        ResponseType responseType = process(processor, operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
        assertEquals(processor.operationType, OperationType.TERMINATE_WORKER);
    }

    private final class IntegrationTestOperationProcessor extends AbstractOperationProcessor {

        private OperationType operationType;

        @Override
        protected void processOperation(OperationType operationType, SimulatorOperation op,
                                        SimulatorAddress sourceAddress, Promise promise) throws Exception {
            this.operationType = operationType;
            promise.answer(ResponseType.SUCCESS);
        }
    }
}
